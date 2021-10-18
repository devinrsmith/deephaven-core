/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.table.TableSpec;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.util.Collections;

public class BarrageSession extends FlightSession implements BarrageSubscription.Factory {

    public static BarrageSession of(
            SessionImpl session, BufferAllocator incomingAllocator, ManagedChannel channel) {
        final FlightClient client = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
                incomingAllocator, channel, Collections.singletonList(new SessionMiddleware(session)));
        return new BarrageSession(session, client, channel);
    }

    private final Channel interceptedChannel;

    protected BarrageSession(
            final SessionImpl session, final FlightClient client, final ManagedChannel channel) {
        super(session, client);
        this.interceptedChannel = ClientInterceptors.intercept(channel, new AuthInterceptor());
    }

    private TableHandleManager manager() {
        return session();
    }

    @Override
    public BarrageSubscription subscribe(
            final TableSpec tableSpec, final BarrageSubscriptionOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = manager().execute(tableSpec)) {
            return subscribe(handle, options);
        }
    }

    @Override
    public BarrageSubscription subscribe(
            final TableDefinition tableDefinition, final TableSpec tableSpec, final BarrageSubscriptionOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = manager().execute(tableSpec)) {
            return subscribe(tableDefinition, handle, options);
        }
    }

    @Override
    public BarrageSubscription subscribe(
            final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        final TableDefinition tableDefinition;
        if (true) {
            tableDefinition = BarrageUtil.convertArrowSchema(schema(tableHandle)).tableDef;
        } else if (false) {
            final ByteBuffer bb = tableHandle.response().getSchemaHeader().asReadOnlyByteBuffer();
            bb.position(bb.position() + 8);
            tableDefinition = BarrageUtil.convertArrowSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(bb)).tableDef;
        } else if (false) {
            final ByteBuffer bb = tableHandle.response().getSchemaHeader().asReadOnlyByteBuffer();
            bb.position(bb.position() + 8);
            tableDefinition = BarrageUtil.convertArrowSchema(Schema.deserialize(bb)).tableDef;
        } else {
            throw new IllegalStateException();
        }
        return subscribe(tableDefinition, tableHandle, options);
    }

    @Override
    public BarrageSubscription subscribe(
            final TableDefinition tableDefinition, final TableHandle tableHandle,
            final BarrageSubscriptionOptions options) {
        final TableHandle handleForSubscription = tableHandle.newRef();
        return new BarrageSubscriptionImpl(this, handleForSubscription, options, tableDefinition);
    }

    public Channel channel() {
        return interceptedChannel;
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    final AuthenticationInfo localAuth = ((SessionImpl) session()).auth();
                    headers.put(Metadata.Key.of(localAuth.sessionHeaderKey(), Metadata.ASCII_STRING_MARSHALLER),
                            localAuth.session());
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
