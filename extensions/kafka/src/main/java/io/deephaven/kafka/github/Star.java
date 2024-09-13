//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.github;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Star {

    private final String action;
    private final Instant starredAt;
    private final Repository repository;
    private final Organization organization;
    private final Sender sender;

    @JsonCreator
    public Star(
            @JsonProperty("action") String action,
            @JsonProperty("starred_at") Instant starredAt,
            @JsonProperty("repository") Repository repository,
            @JsonProperty("organization") Organization organization,
            @JsonProperty("sender") Sender sender) {
        this.action = action;
        this.starredAt = starredAt;
        this.repository = repository;
        this.organization = organization;
        this.sender = sender;
    }

    public String action() {
        return action;
    }

    public Instant starredAt() {
        return starredAt;
    }

    public Repository repository() {
        return repository;
    }

    public Organization organization() {
        return organization;
    }

    public Sender sender() {
        return sender;
    }
}
