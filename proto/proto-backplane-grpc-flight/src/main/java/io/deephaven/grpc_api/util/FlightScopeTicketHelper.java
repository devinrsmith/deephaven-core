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
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Ticket ticket, final String logId) {
        return scopeIdToDescriptor(ScopeTicketHelper.ticketToScopeId(ticket, logId));
    }

}
