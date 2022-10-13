package edu.utah.kmm.va.cdshooks.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.opencds.config.api.model.FhirVersion;
import org.opencds.hooks.model.context.WritableHookContext;
import org.opencds.hooks.model.json.CdsHooksJsonUtil;
import org.opencds.hooks.model.request.CdsRequest;
import org.opencds.hooks.model.request.WritableCdsRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class CDSHooksProxy {

    private enum RequestState {PENDING, COMPLETED, ERROR, DEQUEUED, HANDLED}

    private static class BatchRequestEntry {

        private final String hookId;

        private final String hookInstance;

        private RequestState state = RequestState.PENDING;

        private String cdsResponse;

        BatchRequestEntry(
                String hookId,
                String hookInstance) {
            this.hookId = hookId;
            this.hookInstance = hookInstance;
        }

    }

    private static class BatchRequest {

        private final List<BatchRequestEntry> entries = new ArrayList<>();

        private final String requestId;

        private boolean aborted;

        BatchRequest(String requestId) {
            this.requestId = requestId;
        }

        private BatchRequestEntry findEntry(Predicate<BatchRequestEntry> predicate) {
            return entries.stream().filter(predicate).findFirst().orElse(null);
        }

        synchronized void onComplete(
                String hookInstance,
                HttpResponse response) {
            if (aborted) {
                return;
            }

            BatchRequestEntry entry = findEntry(e -> e.hookInstance.equals(hookInstance));

            if (entry == null) {
                return;
            }

            if (response.getStatusLine().getStatusCode() / 100 != 2) {
                logError(entry, "Http error code " + response.getStatusLine().getStatusCode());
                return;
            }

            try {
                entry.state = RequestState.COMPLETED;
                entry.cdsResponse = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
                log.debug(entry.cdsResponse);
            } catch (Exception e) {
                logError(entry, e.getMessage());
            }

        }

        private void logError(
                BatchRequestEntry entry,
                String message) {
            log.error(String.format("Error executing CDSHook service %s.  The error was: %s", entry.hookId, message));
            entry.state = RequestState.ERROR;
            entries.remove(entry);
        }

        synchronized String getNextHookInstance() {
            BatchRequestEntry entry = findEntry(r -> r.state == RequestState.COMPLETED);

            if (entry != null) {
                entry.state = RequestState.DEQUEUED;
                return entry.hookInstance;
            }

            return null;
        }


        synchronized String getCdsResponse(String hookInstance) {
            BatchRequestEntry entry = findEntry(e -> e.hookInstance.equals(hookInstance) && e.state == RequestState.DEQUEUED);

            if (entry != null) {
                entry.state = RequestState.HANDLED;
                entries.remove(entry);
                return entry.cdsResponse;
            }

            return null;
        }

        synchronized void addRequest(
                String hookId,
                String hookInstance) {
            entries.add(new BatchRequestEntry(hookId, hookInstance));
        }

        boolean allComplete() {
            return entries.isEmpty();
        }

        void abort() {
            aborted = true;
            entries.clear();
        }

    }

    private static final Log log = LogFactory.getLog(CDSHooksProxy.class);

    private static final PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();

    private static final Pattern pattern = Pattern.compile("^.*\\.[^\\\\]+$");

    private final Map<String, BatchRequest> batchRequestMap = new ConcurrentHashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClients.createDefault();

    private volatile CDSHooksCatalog cdsHooksCatalog;

    @Value("${maxThreads:10}")
    private int maxThreads;

    @Value("${cdsHooksEndpoint}")
    private String cdsHooksEndpoint;

    @Value("${fhirEndpoint}")
    private String fhirEndpoint;

    private URL fhirEndpointURL;

    private ThreadPoolExecutor threadPoolExecutor;

    @POST
    @Path("/forward")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response post(MultivaluedMap<String, String> data) {
        String batchId = queueServices(data);
        return Response.ok(batchId).build();
    }

    @GET
    @Path("/next/{batchId}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getNextHookInstance(@PathParam("batchId") String batchId) {
        BatchRequest batchRequest = batchRequestMap.get(batchId);

        if (batchRequest == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else if (batchRequest.allComplete()) {
            batchRequestMap.remove(batchId);
            return Response.noContent().build();
        } else {
            String response = batchRequest.getNextHookInstance();
            return Response.ok(response == null ? "" : response).build();
        }
    }

    @GET
    @Path("/response/{batchId}/{hookInstance}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCdsResponse(
            @PathParam("batchId") String batchId,
            @PathParam("hookInstance") String hookInstance) {
        BatchRequest batchRequest = batchRequestMap.get(batchId);
        String response = batchRequest == null ? null : batchRequest.getCdsResponse(hookInstance);

        if (response == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else {
            return Response.ok(response).build();
        }
    }

    @GET
    @Path("/abort/{batchId}")
    public Response abortRequest(@PathParam("batchId") String batchId) {
        BatchRequest batchRequest = batchRequestMap.remove(batchId);

        if (batchRequest != null) {
            batchRequest.abort();
        }

        return (batchRequest == null ? Response.notModified() : Response.ok()).build();
    }

    @GET
    @Path("/static/{resource:.*}")
    public Response staticResource(
            @Context ServletContext servletContext,
            @Context UriInfo uriInfo,
            @PathParam("resource") String resource) {
        try {
            String contentType = null;

            if (!pattern.matcher(resource).matches()) {
                URI redirectUri = uriInfo.getRequestUriBuilder().path("index.html").build();
                return Response.temporaryRedirect(redirectUri).build();
            } else if (resource.endsWith(".js")) {
                contentType = "application/javascript";
            }

            InputStream is = servletContext.getResourceAsStream("/WEB-INF/static/" + resource);
            is = is == null ? resourceResolver.getResource("classpath:static/" + resource).getInputStream() : is;
            return Response.ok().entity(is)
                    .header(HttpHeaders.CONTENT_TYPE, contentType)
                    .header(HttpHeaders.CONTENT_LOCATION, uriInfo.getRequestUri())
                    .build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    private String queueServices(MultivaluedMap<String, String> data) {
        BatchRequest batch = newBatchRequest(data.getFirst("handle"));
        String hook = data.getFirst("hook");
        Validate.notNull(hook, "No hook type specified in request.");
        Map<String, String> context = new HashMap<>();
        copyMap(data, context, "patientId", "userId");
        getCatalog().getServices("patient-view").forEach(s -> queueService(batch, s, context));
        return batch.allComplete() ? null : batch.requestId;
    }

    private BatchRequest newBatchRequest(String requestId) {
        synchronized (batchRequestMap) {
            BatchRequest batchRequest = new BatchRequest(requestId);
            batchRequestMap.put(requestId, batchRequest);
            return batchRequest;
        }
    }

    private void copyMap(
            MultivaluedMap<String, String> from,
            Map<String, String> to,
            String... fields) {
        Arrays.stream(fields).forEach(field -> to.put(field, from.getFirst(field)));
    }

    private void queueService(
            BatchRequest batch,
            CDSHooksService service,
            Map<String, ?> context) {
        WritableCdsRequest request = new WritableCdsRequest();
        request.setHook(service.getHook());
        WritableHookContext hookContext = new WritableHookContext();
        hookContext.setHook(service.getHook());
        context.forEach(hookContext::add);
        request.setContext(hookContext);
        request.setHookInstance(UUID.randomUUID().toString());
        request.setFhirServer(fhirEndpointURL);
        request.setFhirVersion(FhirVersion.R4);
        batch.addRequest(service.getId(), request.getHookInstance());
        threadPoolExecutor.execute(() -> requestExecution(service, request, batch::onComplete));
    }

    @PostConstruct
    private void initialize() throws MalformedURLException {
        fhirEndpointURL = new URL(fhirEndpoint);
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
    }

    public CDSHooksCatalog getCatalog() {
        return cdsHooksCatalog == null ? loadCatalog() : cdsHooksCatalog;
    }

    private synchronized CDSHooksCatalog loadCatalog() {
        if (cdsHooksCatalog == null) {
            try {
                HttpGet request = new HttpGet(cdsHooksEndpoint);
                log.debug(cdsHooksEndpoint);
                HttpResponse response = httpClient.execute(request);
                String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
                log.debug(body);
                cdsHooksCatalog = mapper.readValue(body, CDSHooksCatalog.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return cdsHooksCatalog;
    }

    private void requestExecution(
            CDSHooksService service,
            CdsRequest request,
            BiConsumer<String, HttpResponse> responseHandler) {
        try {
            HttpPost httpPost = new HttpPost(cdsHooksEndpoint + "/" + service.getId());
            httpPost.setEntity(new StringEntity(new CdsHooksJsonUtil().toJson(request)));
            httpPost.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            httpPost.addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            HttpResponse response = httpClient.execute(httpPost);
            responseHandler.accept(request.getHookInstance(), response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
