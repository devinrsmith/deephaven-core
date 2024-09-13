//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.github;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sender {

    private final String login;
    private final String avatarUrl;

    @JsonCreator
    public Sender(
            @JsonProperty("login") String login,
            @JsonProperty("avatar_url") String avatarUrl) {
        this.login = login;
        this.avatarUrl = avatarUrl;
    }

    public String login() {
        return login;
    }

    public String avatarUrl() {
        return avatarUrl;
    }
}
