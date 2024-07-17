//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.apache.arrow.flight.FlightStatusCode;

public class Exceptions {
    public static StatusRuntimeException statusRuntimeException(final Code statusCode,
            final String details) {
        return StatusProto.toStatusRuntimeException(
                Status.newBuilder().setCode(statusCode.getNumber()).setMessage(details).build());
    }

    public static StatusRuntimeException statusRuntimeException(final FlightStatusCode statusCode,
            final String details) {
        return StatusProto.toStatusRuntimeException(
                Status.newBuilder().setCode(statusCode.ordinal()).setMessage(details).build());
    }
}
