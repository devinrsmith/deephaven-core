package io.deephaven.grpc_api.util;

import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;

public class SchemaHelper {

    public static int offset = 32; // for debugging

    /**
     * Creates a schema from an export response.
     *
     * @param response the response
     * @return the schema
     */
    public static Schema schema(ExportedTableCreationResponse response) {
        final ByteBuffer bb = response.getSchemaHeader().asReadOnlyByteBuffer();
        // TODO: parse these bytes better?
        if (bb.remaining() < offset) {
            throw new IllegalArgumentException("Not enough bytes for Schema");
        }
        bb.position(bb.position() + offset);
        return Schema.deserialize(bb);
    }
}
