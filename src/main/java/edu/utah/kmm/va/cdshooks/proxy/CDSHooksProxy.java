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

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CDSHooksProxy {

    private static class BatchRequest {

        private final List<String> responses = new ArrayList<>();

        private final String id = UUID.randomUUID().toString();

        private int requestCount;

        private boolean aborted;

        void onComplete(HttpResponse response) {
            if (aborted) {
                return;
            }

            synchronized (responses) {
                requestCount--;

                if (response.getStatusLine().getStatusCode() % 100 != 2) {
                    log.error("Service invocation error: " + response.getStatusLine().getStatusCode());
                    return;
                }

                String cdsResponse = getCdsHookResponse(response);

                if (cdsResponse != null) {
                    responses.add(cdsResponse);
                }
            }
        }

        private String getCdsHookResponse(HttpResponse response) {
            try {
                String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
                log.debug(body);
                return body;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return null;
            }
        }

        String getNextResponse() {
            synchronized (responses) {
                if (requestCount <= 0 || responses.isEmpty()) {
                    return null;
                } else {
                    requestCount--;
                    return responses.remove(0);
                }
            }
        }

        void incrementRequestCount() {
            synchronized (responses) {
                requestCount++;
            }
        }

        boolean allComplete() {
            return requestCount == 0 && responses.isEmpty();
        }

        void abort() {
            aborted = true;
            requestCount = 0;
            responses.clear();
        }
    }

    private static final Log log = LogFactory.getLog(CDSHooksProxy.class);

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
    @Consumes(MediaType.TEXT_PLAIN)
    @SuppressWarnings("unchecked")
    public Response post(@Context HttpServletRequest request) throws IOException {
        String body = request.getReader().lines().collect(Collectors.joining("\n"));
        Map<String, String> data = mapper.readValue(body, Map.class);
        BatchRequest batch = queueServices(data);
        return Response.ok(new StringEntity(batch.allComplete() ? "" : batch.id)).build();
    }

    @GET
    @Path("/response/{requestId}")
    public Response getResponse(@PathParam("requestId") String requestId) throws IOException {
        BatchRequest batchRequest = batchRequestMap.get(requestId);

        if (batchRequest == null) {
            return Response.noContent().build();
        } else if (batchRequest.allComplete()) {
            batchRequestMap.remove(requestId);
            return Response.noContent().build();
        } else {
            String response = batchRequest.getNextResponse();
            return Response.ok(new StringEntity(response == null ? "" : response)).build();
        }
    }

    @GET
    @Path("/abort/{requestId}")
    public Response abortRequest(@PathParam("requestId") String requestId) {
        BatchRequest batchRequest = batchRequestMap.remove(requestId);

        if (batchRequest != null) {
            batchRequest.abort();
        }

        return Response.ok(batchRequestMap.remove(requestId)).build();
    }

    private BatchRequest queueServices(Map<String, String> data) {
        BatchRequest batch = newBatchRequest();
        String hook = data.get("hook");
        Validate.notNull(hook, "No hook type specified in request.");
        Map<String, String> context = new HashMap<>();
        copyMap(data, context, "patientId", "userId" );
        getCatalog().getServices("patient-view").forEach(s -> queueService(batch, s, context));
        return batch;
    }

    private BatchRequest newBatchRequest() {
        synchronized (batchRequestMap) {
            BatchRequest batchRequest = new BatchRequest();
            batchRequestMap.put(batchRequest.id, batchRequest);
            return batchRequest;
        }
    }
    private void copyMap(Map<String, String> from, Map<String, String> to, String... fields) {
        Arrays.stream(fields).forEach(field -> to.put(field, from.get(field)));
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
        batch.incrementRequestCount();
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

    private void requestExecution(CDSHooksService service, CdsRequest request, Consumer<HttpResponse> responseHandler) {
        try {
            HttpPost httpPost = new HttpPost(cdsHooksEndpoint + "/" + service.getId());
            httpPost.setEntity(new StringEntity(new CdsHooksJsonUtil().toJson(request)));
            HttpResponse response = httpClient.execute(httpPost);
            responseHandler.accept(response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
