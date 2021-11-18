package io.deephaven.grpc_api.uri;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.SessionState.ExportBuilder;
import io.deephaven.grpc_api.session.TicketResolverBase;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.Exceptions;
import io.deephaven.grpc_api.util.TicketRouterHelper;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.util.auth.AuthContext;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;

public final class UriTicketResolver extends TicketResolverBase {

    private static final char TICKET_PREFIX = 'u';

    private static final String FLIGHT_DESCRIPTOR_ROUTE = "uri";

    private final UriRouter uriRouter;

    @Inject
    public UriTicketResolver(UriRouter uriRouter) {
        super((byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.uriRouter = Objects.requireNonNull(uriRouter);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            final @Nullable SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(session == null ? null : session.getAuthContext(), uriFor(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            final @Nullable SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(session == null ? null : session.getAuthContext(), uriFor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(final AuthContext auth, final URI uri, final String logId) {
        final Object object;
        try {
            object = uriRouter.resolveSafely(auth, uri);
        } catch (InterruptedException e) {
            throw new IllegalArgumentException(e);
        }
        // noinspection unchecked
        return SessionState.wrapAsExport((T) object);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            final @Nullable SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        final URI uri = uriFor(descriptor, logId);
        final Object value;
        try {
            value = uriRouter.resolveSafely(session == null ? null : session.getAuthContext(), uri);
        } catch (InterruptedException e) {
            throw new IllegalArgumentException(e);
        }
        final Flight.FlightInfo info;
        if (value instanceof Table) {
            info = TicketRouter.getFlightInfo((Table) value, descriptor, flightTicketForUri(uri));
        } else {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND, "Could not resolve URI for '" + logId + "': field '" + getLogNameFor(uri) + "' is not a flight");
        }
        return SessionState.wrapAsExport(info);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final ByteBuffer ticket, final String logId) {
        return publish(session, uriFor(ticket, logId));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return publish(session, uriFor(descriptor, logId));
    }

    private <O> ExportBuilder<O> publish(SessionState session, URI uri) {
        final Consumer<O> target;
        try {
            target = uriRouter.publishTarget(session.getAuthContext(), uri);
        } catch (UnsupportedOperationException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, e.getMessage());
        }
        final ExportBuilder<O> resultBuilder = session.nonExport();
        final SessionState.ExportObject<O> resultExport = resultBuilder.getExport();
        final ExportBuilder<O> publishTask = session.nonExport();
        publishTask
                .requiresSerialQueue()
                .require(resultExport)
                .submit(() -> target.accept(resultExport.get()));
        return resultBuilder;
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return getLogNameFor(uriFor(ticket, logId));
    }

    private static String getLogNameFor(URI uri) {
        return uri.toString();
    }

    @Override
    public void forAllFlightInfo(@Nullable SessionState session, Consumer<Flight.FlightInfo> visitor) {
        final AuthContext auth = session == null ? null : session.getAuthContext();
        for (UriResolver resolver : uriRouter.resolvers()) {
            resolver.forAllUrisSafely(auth, (uri, value) -> {
                if (!(value instanceof Table)) {
                    return;
                }
                visitor.accept(TicketRouter.getFlightInfo((Table) value, descriptorForUri(uri), flightTicketForUri(uri)));
            });
        }
    }

    /**
     * Convenience method to convert from a {@code uri} to Ticket.
     *
     * @param uri the URI
     * @return the ticket this descriptor represents
     */
    public static Ticket ticketForUri(URI uri) {
        final byte[] ticket = (TICKET_PREFIX + "/" + uri).getBytes(StandardCharsets.UTF_8);
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a {@code uri} to Flight.Ticket.
     *
     * @param uri the URI
     * @return the ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForUri(URI uri) {
        final byte[] ticket = (TICKET_PREFIX + "/" + uri).getBytes(StandardCharsets.UTF_8);
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a {@code uri} to Flight.FlightDescriptor.
     *
     * @param uri the URI
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForUri(URI uri) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addPath(FLIGHT_DESCRIPTOR_ROUTE)
                .addPath(uri.toString())
                .build();
    }

    private URI uriFor(final ByteBuffer ticket, final String logId) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': ticket not supplied");
        }
        if (ticket.remaining() < 5) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': ticket too small");
        }
        final String ticketAsString;
        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticketAsString = decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }
        if (!ticketAsString.startsWith("u/")) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': improper prefix");
        }
        try {
            return URI.create(ticketAsString.substring(2));
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': ticket does conform to URI");
        }
    }

    private URI uriFor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': descriptor not supplied");
        }
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': only flight paths are supported");
        }
        // current structure: uri/<uri>
        if (descriptor.getPathCount() != 2) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }
        if (!FLIGHT_DESCRIPTOR_ROUTE.equals(descriptor.getPath(0))) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': unexpected path '" + descriptor.getPath(0) + '\'');
        }
        try {
            return URI.create(descriptor.getPath(1));
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve URI for '" + logId + "': path does conform to URI");
        }
    }
}
