//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsoleServiceTest extends DeephavenApiServerSingleAuthenticatedBase {

    private class Observer implements ClientResponseObserver<LogSubscriptionRequest, LogSubscriptionData> {
        private final CountDownLatch onNext;
        private final CountDownLatch onDone;
        private ClientCallStreamObserver<?> stream;
        private volatile Throwable error;

        public Observer(int expected) {
            onNext = new CountDownLatch(expected);
            onDone = new CountDownLatch(1);
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<LogSubscriptionRequest> requestStream) {
            this.stream = requestStream;
        }

        @Override
        public void onNext(LogSubscriptionData value) {
            if (onNext.getCount() == 0) {
                throw new IllegalStateException("Expected latch count exceeded");
            }
            onNext.countDown();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            onDone.countDown();
        }

        @Override
        public void onCompleted() {
            onDone.countDown();
        }

        void cancel(String message, Throwable cause) {
            stream.cancel(message, cause);
        }

        void subscribeToLogs() {
            channel().console().subscribeToLogs(LogSubscriptionRequest.getDefaultInstance(), this);
        }

        void awaitRpcEstablished(Duration duration) throws InterruptedException, TimeoutException {
            // There is no other way afaict (at least w/ the observer interfaces that gRPC libraries provide) to know
            // that an RPC has been established _besides_ waiting for an onNext message.
            assertThat(onNext.getCount()).isEqualTo(1);
            logBuffer().record(record(Instant.now(), LogLevel.STDOUT, "hello, world!"));
            awaitOnNext(duration);
        }

        void awaitOnNext(Duration duration) throws InterruptedException, TimeoutException {
            if (!onNext.await(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                cancel("onNext latch timed out", null);
                throw new TimeoutException();
            }
        }

        void awaitOnDone(Duration duration) throws InterruptedException, TimeoutException {
            if (!onDone.await(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                cancel("onDone latch timed out", null);
                throw new TimeoutException();
            }
        }
    }

    @Test
    public void subscribeToLogsHistory() throws InterruptedException, TimeoutException {
        logBuffer().record(record(Instant.now(), LogLevel.STDOUT, "hello"));
        logBuffer().record(record(Instant.now(), LogLevel.STDOUT, "world"));
        final Observer observer = new Observer(2);
        observer.subscribeToLogs();
        observer.awaitOnNext(Duration.ofSeconds(3));
        observer.cancel("done", null);
        observer.awaitOnDone(Duration.ofSeconds(3));
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        assertThat(observer.error).hasMessage("CANCELLED: done");
    }

    @Test
    public void subscribeToLogsInline() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(2);
        observer.subscribeToLogs();
        logBuffer().record(record(Instant.now(), LogLevel.STDOUT, "hello"));
        logBuffer().record(record(Instant.now(), LogLevel.STDOUT, "world"));
        observer.awaitOnNext(Duration.ofSeconds(3));
        observer.cancel("done", null);
        observer.awaitOnDone(Duration.ofSeconds(3));
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        assertThat(observer.error).hasMessage("CANCELLED: done");
    }

    @Test
    public void closingSessionCancelsSubscribeToLogs() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(1);
        observer.subscribeToLogs();
        observer.awaitRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        assertThat(observer.error).hasMessage("CANCELLED: Session closed");
    }

    private static LogBufferRecord record(Instant timestamp, LogLevel level, String message) {
        final LogBufferRecord record = new LogBufferRecord();
        record.setTimestampMicros(timestamp.toEpochMilli() * 1000);
        record.setLevel(level);
        record.setData(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));
        return record;
    }
}
