package io.deephaven.grpc_api.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteOrder;

public class FlightScopeTicketHelper {
    /**
     * Convenience method to convert from scope id to {@link Flight.Ticket}.
     *
     * @param scopeId the scope id
     * @return a grpc Ticket wrapping the scope id
     */
    public static Flight.Ticket scopeIdToFlightTicket(String scopeId) {
        final byte[] dest = ScopeTicketHelper.scopeIdToBytes(scopeId);
        return Flight.Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

    /**
     * Convenience method to convert from scope id to {@link Flight.FlightDescriptor}.
     *
     * @param scopeId the scope id
     * @return a grpc Ticket wrapping the scope id
     */
    public static Flight.FlightDescriptor scopeIdToDescriptor(String scopeId) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addPath(ScopeTicketHelper.FLIGHT_DESCRIPTOR_ROUTE)
                .addPath(scopeId)
                .build();
    }

    /**
     * Convenience method to convert from {@link Flight.Ticket} to scope id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ScopeTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int scope id in
     * little-endian.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the scope id that the Ticket wraps
     */
    public static String ticketToScopeId(final Flight.Ticket ticket, final String logId) {
        return ScopeTicketHelper.ticketToScopeIdInternal(ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    /**
     * Convenience method to convert from {@link Flight.FlightDescriptor} to scope id.
     *
     * <p>
     * Descriptor must be a path.
     *
     * @param descriptor the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the scope id that the Ticket wraps
     */
    public static String descriptorToScopeId(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': is empty");
        }
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': not a path");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }
        return descriptor.getPath(1);
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Ticket ticket, final String logId) {
        return scopeIdToDescriptor(ScopeTicketHelper.ticketToScopeId(ticket, logId));
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return scopeIdToDescriptor(ticketToScopeId(ticket, logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToFlightTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return scopeIdToFlightTicket(descriptorToScopeId(descriptor, logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return ScopeTicketHelper.wrapScopeIdInTicket(descriptorToScopeId(descriptor, logId));
    }
}
