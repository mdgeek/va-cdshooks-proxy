package edu.utah.kmm.va.cdshooks.proxy;

import ca.uhn.fhir.util.UrlUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;

import javax.ws.rs.core.MultivaluedMap;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

class BatchRequest {

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

    private static final Log log = LogFactory.getLog(BatchRequest.class);

    private final List<BatchRequestEntry> entries = new ArrayList<>();

    private final String requestId;

    private final String hook;

    private final URL fhirEndpoint;

    private final Map<String, String> context = new HashMap<>();

    private boolean aborted;

    BatchRequest(MultivaluedMap<String, String> data) {
        requestId = data.getFirst("handle");
        hook = data.getFirst("hook");
        fhirEndpoint = toURL(data.getFirst("fhir_endpoint"));
        Validate.notNull(hook, "No hook type specified in request.");
        copyMap(data, context, "patientId", "userId");
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

    private void copyMap(
            MultivaluedMap<String, String> from,
            Map<String, String> to,
            String... fields) {
        Arrays.stream(fields).forEach(field -> to.put(field, from.getFirst(field)));
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
