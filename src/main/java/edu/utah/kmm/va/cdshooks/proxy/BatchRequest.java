package edu.utah.kmm.va.cdshooks.proxy;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;

import javax.ws.rs.core.MultivaluedMap;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

class BatchRequest {

    private enum RequestState {PENDING, COMPLETED, ERROR, DEQUEUED, HANDLED}

    public static class InstanceHandle {

        public final String hookId;

        public final String hookInstance;

        InstanceHandle(
                String hookId,
                String hookInstance) {
            this.hookId = hookId;
            this.hookInstance = hookInstance;
        }

        @Override
        public String toString() {
            return hookId + ":" + hookInstance;
        }

        public String toJSON() {
            return String.format("{\"hookId\":\"%s\",\"hookInstance\":\"%s\"}", hookId, hookInstance);
        }
    }

    public static class BatchRequestEntry {

        public final InstanceHandle instanceHandle;

        private RequestState state = RequestState.PENDING;

        private String cdsResponse;

        BatchRequestEntry(InstanceHandle instanceHandle) {
            this.instanceHandle = instanceHandle;
        }

    }

    private static final Log log = LogFactory.getLog(BatchRequest.class);

    private final List<BatchRequestEntry> entries = new ArrayList<>();

    private final String requestId;

    private final String hook;

    private final URL fhirEndpoint;

    private final Map<String, String> parameters = new HashMap<>();

    private final Map<String, String> context;

    private boolean aborted;

    BatchRequest(MultivaluedMap<String, String> params) {
        params.keySet().forEach(k -> this.parameters.put(k, params.getFirst(k)));
        requestId = parameters.get("handle");
        hook = parameters.get("hook");
        fhirEndpoint = toURL(parameters.get("fhir_endpoint"));
        Validate.notNull(hook, "No hook type specified in request.");
        context = createContext("patientId", "userId", "orderId");
    }

    private URL toURL(String url) {
        try {
            return url == null ? null : new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getRequestId() {
        return requestId;
    }

    public String getHook() {
        return hook;
    }

    public URL getFhirEndpoint() {
        return fhirEndpoint;
    }

    public Map<String, String> getContext() {
        return Collections.unmodifiableMap(context);
    }

    public Map<String, String> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }

    private Map<String, String> createContext(String... fields) {
        Map<String, String> context = new HashMap<>();
        Arrays.stream(fields).forEach(field -> {
            String value = parameters.get(field);

            if (value != null) {
                context.put(field, value);
            }
        });
        return context;
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

        BatchRequestEntry entry = findEntry(e -> e.instanceHandle.hookInstance.equals(hookInstance));

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
        log.error(String.format("Error executing CDSHook service %s.  The error was: %s", entry.instanceHandle, message));
        entry.state = RequestState.ERROR;
        entries.remove(entry);
    }

    synchronized InstanceHandle getNextHookInstance() {
        BatchRequestEntry entry = findEntry(r -> r.state == RequestState.COMPLETED);

        if (entry != null) {
            entry.state = RequestState.DEQUEUED;
            return entry.instanceHandle;
        }

        return null;
    }


    synchronized String getCdsResponse(String hookInstance) {
        BatchRequestEntry entry = findEntry(e -> e.instanceHandle.hookInstance.equals(hookInstance) && e.state == RequestState.DEQUEUED);

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
        entries.add(new BatchRequestEntry(new InstanceHandle(hookId, hookInstance)));
    }

    boolean allComplete() {
        return entries.isEmpty();
    }

    void abort() {
        aborted = true;
        entries.clear();
    }

}
