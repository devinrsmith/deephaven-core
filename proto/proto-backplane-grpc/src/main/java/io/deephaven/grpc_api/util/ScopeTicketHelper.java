/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class ScopeTicketHelper {
    public static final byte TICKET_PREFIX = 's';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "scope";

    /**
     * Convenience method to convert from scope id to {@link Ticket}.
     *
     * @param scopeId the scope id
     * @return a grpc Ticket wrapping the scope id
     */
    public static Ticket wrapScopeIdInTicket(String scopeId) {
        final byte[] dest = scopeIdToBytes(scopeId);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

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

    /**
     * Convenience method to convert from {@link ByteBuffer} to scope id. Most efficient when {@code ticket} is
     * {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ScopeTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int scope id in
     * little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the scope id that the Ticket wraps
     */
    public static String ticketToScopeId(final ByteBuffer ticket, final String logId) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': ticket not supplied");
        }
        return ticketToScopeIdInternal(ticket, logId);
    }

    /**
     * Convenience method to convert from {@link ByteBuffer} to scope ticket. Most efficient when {@code ticket} is
     * {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ScopeTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int scope id in
     * little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @return the scope id that the Ticket wraps
     */
    public static Ticket wrapScopeIdInTicket(final ByteBuffer ticket) {
        final ByteBuffer lebb = ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticket
                : ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(lebb)).build();
    }

    /**
     * Convenience method to create a human readable string from the flight ticket.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final Ticket ticket, final String logId) {
        return toReadableString(
                ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    /**
     * Convenience method to create a human readable string from a table reference.
     *
     * @param tableReference the table reference
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final TableReference tableReference, final String logId) {
        switch (tableReference.getRefCase()) {
            case TICKET:
                return toReadableString(tableReference.getTicket(), logId);
            case BATCH_OFFSET:
                return String.format("batchOffset[%d]", tableReference.getBatchOffset());
            default:
                throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Could not resolve '" + logId + "': unexpected TableReference type '"
                                + tableReference.getRefCase() + "'");
        }
    }

    /**
     * Convenience method to create a human readable string from the flight ticket (as ByteBuffer). Most efficient when
     * {@code ticket} is {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + ticketToScopeId(ticket, logId);
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
