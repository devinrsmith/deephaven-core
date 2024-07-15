//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ReplicateVectors {
    private static final String TASK = "replicateVectors";

    @SuppressWarnings("AutoBoxing")
    public static void main(String[] args) throws IOException {
        Map<String, Long> serialVersionUIDs = Collections.emptyMap();

        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVector.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorDirect.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorSlice.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharSubVector.java",
                serialVersionUIDs);
    }
}
