//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.protobuf.ByteString;
import io.deephaven.plugin.EchoObjectType;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConnectRequest;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportNotificationRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.TerminationNotificationRequest;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionServiceCloseTest extends DeephavenApiServerSingleAuthenticatedBase {


    final class ListFieldsObserver extends OnDoneObserver<ListFieldsRequest, FieldsChangeUpdate> {

        @Override
        void sendImpl() {
            channel().application().listFields(ListFieldsRequest.getDefaultInstance(), this);
        }
    }

    @Test
    public void listFields() throws ExecutionException, InterruptedException, TimeoutException {
        final ListFieldsObserver observer = new ListFieldsObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    final class ExportNotificationsObserver extends OnDoneObserver<ExportNotificationRequest, ExportNotification> {

        @Override
        void sendImpl() {
            channel().session().exportNotifications(ExportNotificationRequest.getDefaultInstance(), this);
        }
    }

    @Test
    public void exportNotifications() throws ExecutionException, InterruptedException, TimeoutException {
        final ExportNotificationsObserver observer = new ExportNotificationsObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertCompleted();
    }

    final class ExportedTableUpdatesObserver extends OnDoneObserver<ExportedTableUpdatesRequest, ExportedTableUpdateMessage> {

        @Override
        void sendImpl() {
            channel().table().exportedTableUpdates(ExportedTableUpdatesRequest.getDefaultInstance(), this);
        }
    }

//    @Ignore
//    @Test
//    public void exportedTableUpdates() throws ExecutionException, InterruptedException, TimeoutException {
//        final ExportedTableUpdatesObserver observer = new ExportedTableUpdatesObserver();
//        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
//        closeSession();
//        observer.awaitOnDone(Duration.ofSeconds(3));
//        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
//    }

    final class MessageStreamNoConnectRequestObserver extends OnDoneObserver<StreamRequest, StreamResponse> {

        @Override
        void sendImpl() {
            channel().object().messageStream(this);
        }
    }

    final class MessageStreamObserver extends OnDoneObserver<StreamRequest, StreamResponse> {

        private final CountDownLatch onNextLatch = new CountDownLatch(1);
        private final Ticket ticket;

        private StreamResponse onNext;

        public MessageStreamObserver(Ticket ticket) {
            this.ticket = ticket;
        }

        @Override
        void sendImpl() throws InterruptedException, TimeoutException {
            final StreamObserver<StreamRequest> stream = channel().object().messageStream(this);
            stream.onNext(StreamRequest.newBuilder()
                    .setConnect(ConnectRequest.newBuilder()
                            .setSourceId(TypedTicket.newBuilder()
                                    .setType(EchoObjectType.NAME)
                                    .setTicket(ticket)
                                    .build())
                            .build())
                    .build());
            // The state management wrt ObjectService is a bit more complex.
            if (!onNextLatch.await(3, TimeUnit.SECONDS)) {
                if (t != null) {
                    throw new RuntimeException(t);
                }
                throw new TimeoutException();
            }
            // We're awaiting the success of the connect
            assertThat(onNext.getData().getPayload()).isEqualTo(ByteString.empty());
            assertThat(onNext.getData().getExportedReferencesCount()).isZero();
        }

        @Override
        void onNextImpl(StreamResponse value) {
            this.onNext = value;
            onNextLatch.countDown();
        }
    }

    @Test
    public void messageStreamNoConnectRequest() throws ExecutionException, InterruptedException, TimeoutException {
        final MessageStreamNoConnectRequestObserver observer = new MessageStreamNoConnectRequestObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    @Test
    public void messageStream() throws ExecutionException, InterruptedException, TimeoutException {
        final ExportObject<Object> export = authenticatedSessionState().newServerSideExport(EchoObjectType.INSTANCE);
        final MessageStreamObserver observer = new MessageStreamObserver(export.getExportId());
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    final class SubscribeToLogsObserver extends OnDoneObserver<LogSubscriptionRequest, LogSubscriptionData> {

        @Override
        void sendImpl() {
            channel().console().subscribeToLogs(LogSubscriptionRequest.getDefaultInstance(), this);
        }
    }

    @Test
    public void subscribeToLogs() throws ExecutionException, InterruptedException, TimeoutException {
        final SubscribeToLogsObserver observer = new SubscribeToLogsObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    final class AutoCompleteStreamObserver extends OnDoneObserver<AutoCompleteRequest, AutoCompleteResponse> {

        @Override
        void sendImpl() {
            channel().console().autoCompleteStream(this);
        }
    }

    @Test
    public void autoCompleteStream() throws ExecutionException, InterruptedException, TimeoutException {
        final AutoCompleteStreamObserver observer = new AutoCompleteStreamObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    final class TerminationObserver
            extends OnDoneObserver<TerminationNotificationRequest, TerminationNotificationResponse> {

        @Override
        void sendImpl() {
            channel().session().terminationNotification(TerminationNotificationRequest.getDefaultInstance(), this);
        }
    }

    @Test
    public void terminationNotification() throws InterruptedException, TimeoutException, ExecutionException {
        final TerminationObserver observer = new TerminationObserver();
        observer.sendAndWaitForRpcEstablished(Duration.ofSeconds(3));
        closeSession();
        observer.awaitOnDone(Duration.ofSeconds(3));
        observer.assertError(StatusRuntimeException.class, "CANCELLED: Session closed");
//        observer.assertError(StatusRuntimeException.class, "UNAUTHENTICATED: Session has ended");
    }

    abstract class OnDoneObserver<ReqT, RespT> implements ClientResponseObserver<ReqT, RespT> {
        ClientCallStreamObserver<ReqT> observer;
        Throwable t;
        boolean onCompleted;
        final CountDownLatch onDone = new CountDownLatch(1);

        abstract void sendImpl() throws InterruptedException, TimeoutException;

        void onNextImpl(RespT value) {
            // Most cases don't really care about responses, mainly testing out onDone behavior
        }

        final void sendAndWaitForRpcEstablished(Duration duration)
                throws ExecutionException, InterruptedException, TimeoutException {
            sendImpl();
            // This is a bit hacky, but allows us to know the earlier RPC has been established.
            channel()
                    .configFuture()
                    .getAuthenticationConstants(AuthenticationConstantsRequest.getDefaultInstance())
                    .get(duration.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public final void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
            this.observer = requestStream;
        }

        @Override
        public final void onNext(RespT value) {
            onNextImpl(value);
        }

        @Override
        public final void onError(Throwable t) {
            this.t = t;
            onDone.countDown();
        }

        @Override
        public final void onCompleted() {
            onCompleted = true;
            onDone.countDown();
        }

        final void awaitOnDone(Duration duration) throws InterruptedException, TimeoutException {
            if (!onDone.await(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException();
            }
        }

        final void assertCompleted() {
            assertThat(onCompleted).isTrue();
            assertThat(t).isNull();
        }

        final void assertError(Class<? extends Throwable> exceptionType, String message) {
            assertThat(onCompleted).isFalse();
            assertThat(t).isNotNull();
            assertThat(t).isInstanceOf(exceptionType);
            assertThat(t).hasMessage(message);
        }
    }
}
