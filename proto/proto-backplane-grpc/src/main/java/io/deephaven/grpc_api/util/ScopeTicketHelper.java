/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.Ticket;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ScopeTicketHelper {
    public static final byte TICKET_PREFIX = 's';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "scope";

    /**
     * Convenience method to convert from {@link Ticket} to scope id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ScopeTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int scope id in
     * little-endian.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the scope id that the Ticket wraps
     */
    public static String ticketToScopeId(final Ticket ticket, final String logId) {
        return ticketToScopeIdInternal(ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    static byte[] scopeIdToBytes(String scopeId) {
        final byte[] scopeBytes = scopeId.getBytes(StandardCharsets.UTF_8);
        final byte[] dest = new byte[scopeBytes.length + 2];
        dest[0] = TICKET_PREFIX;
        dest[1] = '/';
        System.arraycopy(scopeBytes, 0, dest, 2, scopeBytes.length);
        return dest;
    }

    static String byteBufToHex(final ByteBuffer ticket) {
        StringBuilder sb = new StringBuilder();
        for (int i = ticket.position(); i < ticket.limit(); ++i) {
            sb.append(String.format("%02x", ticket.get(i)));
        }
        return sb.toString();
    }

    static String ticketToScopeIdInternal(final ByteBuffer ticket, final String logId) {
        int pos = ticket.position();
        if (ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': ticket was not provided");
        }
        if (ticket.remaining() < 3) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': ticket too small");
        }
        if (ticket.get(pos) != TICKET_PREFIX || ticket.get(pos + 1) != '/') {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': found 0x" + byteBufToHex(ticket) + " (hex)");
        }
        final int strLen = ticket.remaining() - 2;
        final byte[] bytes = new byte[strLen];
        for (int i = 0; i < strLen; ++i) {
            bytes[i] = ticket.get(pos + i + 2);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
