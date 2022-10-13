package edu.utah.kmm.va.cdshooks.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class CdsHooksService {

    @JsonProperty
    private String hook;

    @JsonProperty
    private String title;

    @JsonProperty
    private String description;

    @JsonProperty
    private String id;

    @JsonProperty
    private Map<String, String> prefetch;

    public CdsHooksService() {

    }

    public String getHook() {
        return hook;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getPrefetch() {
        return prefetch;
    }

}
