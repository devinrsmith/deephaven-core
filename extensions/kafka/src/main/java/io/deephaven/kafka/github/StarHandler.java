//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.github;

import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Sink.StreamKey;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;

import java.time.Instant;

public class StarHandler {

    public static final StreamKey KEY = new StreamKey();

    public static final Key<String> ACTION = Key.of("Action", Type.stringType());
    public static final Key<Instant> STARRED_AT = Key.of("StarredAt", Type.instantType());
    public static final Key<String> REPOSITORY = Key.of("Repository", Type.stringType());
    public static final Key<String> ORGANIZATION = Key.of("Organization", Type.stringType());
    public static final Key<String> SENDER = Key.of("Sender", Type.stringType());
    public static final Key<String> SENDER_AVATAR = Key.of("SenderAvatar", Type.stringType());

    public static StarHandler from(Stream stream) {
        return new StarHandler(
                ObjectAppender.getIfPresent(stream, ACTION),
                InstantAppender.getIfPresent(stream, STARRED_AT),
                ObjectAppender.getIfPresent(stream, REPOSITORY),
                ObjectAppender.getIfPresent(stream, ORGANIZATION),
                ObjectAppender.getIfPresent(stream, SENDER),
                ObjectAppender.getIfPresent(stream, SENDER_AVATAR));
    }

    private final ObjectAppender<String> action;
    private final InstantAppender starredAt;
    private final ObjectAppender<String> repository;
    private final ObjectAppender<String> organization;
    private final ObjectAppender<String> sender;
    private final ObjectAppender<String> senderAvatar;

    private StarHandler(ObjectAppender<String> action, InstantAppender starredAt, ObjectAppender<String> repository,
            ObjectAppender<String> organization, ObjectAppender<String> sender, ObjectAppender<String> senderAvatar) {
        this.action = action;
        this.starredAt = starredAt;
        this.repository = repository;
        this.organization = organization;
        this.sender = sender;
        this.senderAvatar = senderAvatar;
    }

    public void set(Star star) {
        if (action != null) {
            action.set(star.action());
        }
        if (starredAt != null) {
            starredAt.set(star.starredAt());
        }
        if (repository != null) {
            if (star.repository() == null) {
                repository.setNull();
            } else {
                repository.set(star.repository().name());
            }
        }
        if (organization != null) {
            if (star.organization() == null) {
                organization.setNull();
            } else {
                organization.set(star.organization().login());
            }
        }
        if (sender != null) {
            if (star.sender() == null) {
                sender.setNull();
            } else {
                sender.set(star.sender().login());
            }
        }
        if (senderAvatar != null) {
            if (star.sender() == null) {
                senderAvatar.setNull();
            } else {
                senderAvatar.set(star.sender().avatarUrl());
            }
        }
    }
}
