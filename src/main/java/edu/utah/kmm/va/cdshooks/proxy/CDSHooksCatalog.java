package edu.utah.kmm.va.cdshooks.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CDSHooksCatalog {

    @JsonProperty
    private CDSHooksService[] services;

    public CDSHooksCatalog() {
    }

    public CDSHooksService getService(String id) {
        return Arrays.stream(services).filter(s -> Objects.equals(id, s.getId())).findFirst().orElse(null);
    }

    public List<CDSHooksService> getServices(String hookType) {
        return Arrays.stream(services).filter(s -> hookType.equals(s.getHook())).collect(Collectors.toList());
    }
}
