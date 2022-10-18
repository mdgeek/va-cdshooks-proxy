package edu.utah.kmm.va.cdshooks.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

public class CdsHooksProxy {

    private static final Log log = LogFactory.getLog(CdsHooksProxy.class);

    private static final PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();

    private static final Pattern pattern = Pattern.compile("^.*\\.[^\\\\]+$");

    private final Map<String, BatchRequest> batchRequestMap = new ConcurrentHashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClients.createDefault();

    private volatile CdsHooksCatalog cdsHooksCatalog;

    @Value("${maxThreads:10}")
    private int maxThreads;

    @Value("${cdsHooksEndpoint}")
    private String cdsHooksEndpoint;

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
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNextHookInstance(@PathParam("batchId") String batchId) {
        BatchRequest batchRequest = batchRequestMap.get(batchId);

        if (batchRequest == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else if (batchRequest.allComplete()) {
            batchRequestMap.remove(batchId);
            return Response.noContent().build();
        } else {
            BatchRequest.InstanceHandle instanceHandle = batchRequest.getNextHookInstance();
            return Response.ok(instanceHandle == null ? "{}" : instanceHandle.toJSON()).build();
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
        BatchRequest batch = newBatchRequest(data);
        getCatalog().getServices(batch.getHook()).forEach(s -> queueService(batch, s));
        return batch.allComplete() ? null : batch.getRequestId();
    }

    private BatchRequest newBatchRequest(MultivaluedMap<String, String> data) {
        synchronized (batchRequestMap) {
            BatchRequest batchRequest = new BatchRequest(data);
            batchRequestMap.put(batchRequest.getRequestId(), batchRequest);
            return batchRequest;
        }
    }

    private void queueService(
            BatchRequest batch,
            CdsHooksService service) {
        WritableCdsRequest request = new WritableCdsRequest();
        request.setHook(service.getHook());
        WritableHookContext hookContext = new WritableHookContext();
        hookContext.setHook(service.getHook());
        batch.getContext().forEach(hookContext::add);
        request.setContext(hookContext);
        request.setHookInstance(UUID.randomUUID().toString());
        request.setFhirServer(batch.getFhirEndpoint());
        request.setFhirVersion(FhirVersion.R4);
        batch.addRequest(service.getId(), request.getHookInstance());
        threadPoolExecutor.execute(() -> requestExecution(service, request, batch::onComplete));
    }

    @PostConstruct
    private void initialize() {
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
    }

    public CdsHooksCatalog getCatalog() {
        return cdsHooksCatalog == null ? loadCatalog() : cdsHooksCatalog;
    }

    private synchronized CdsHooksCatalog loadCatalog() {
        if (cdsHooksCatalog == null) {
            try {
                HttpGet request = new HttpGet(cdsHooksEndpoint);
                log.debug(cdsHooksEndpoint);
                HttpResponse response = httpClient.execute(request);
                String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
                log.debug(body);
                cdsHooksCatalog = mapper.readValue(body, CdsHooksCatalog.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return cdsHooksCatalog;
    }

    private void requestExecution(
            CdsHooksService service,
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
