//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.github;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.processor.sink.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class WebhookHandler {

    private static final byte[] STAR_EVENT = "star".getBytes(StandardCharsets.UTF_8);

    public static WebhookHandler from(Stream stream, ObjectMapper objectMapper) {
        return new WebhookHandler(StarHandler.from(stream), objectMapper);
    }

    private final StarHandler handler;
    private final ObjectMapper objectMapper;

    WebhookHandler(StarHandler handler, ObjectMapper objectMapper) {
        this.handler = Objects.requireNonNull(handler);
        this.objectMapper = Objects.requireNonNull(objectMapper);
    }

    public boolean handle(ConsumerRecord<?, ?> record) throws IOException {
        final Header githubEvent = record.headers().lastHeader("X-GitHub-Event");
        if (!Arrays.equals(STAR_EVENT, githubEvent.value())) {
            return false;
        }
        final Object value = record.value();
        final Star star;
        if (value instanceof byte[]) {
            star = objectMapper.readValue((byte[]) value, Star.class);
        } else if (value instanceof String) {
            star = objectMapper.readValue((String) value, Star.class);
        } else {
            return false;
        }
        handler.set(star);
        return true;
    }
}
