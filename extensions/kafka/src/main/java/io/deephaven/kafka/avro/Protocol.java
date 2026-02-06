//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.avro;

/**
 * The Protobuf serialization / deserialization protocol.
 *
 * @see #serdes()
 * @see #raw()
 */
public interface Protocol {

    /**
     * The Kafka Avro serdes protocol. The payload's first byte is the serdes magic byte, the next 4-bytes are the
     * schema ID, the next variable-sized bytes are the message indexes, followed by the normal binary encoding of the
     * Avro data.
     *
     * @return the Kafka Avro serdes protocol
     * @see <a href=
     *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html">Kafka
     *      Avro serdes</a>
     * @see <a href=
     *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format">wire-format</a>
     * @see <a href="https://avro.apache.org/">Avro</a>
     */
    static Protocol serdes() {
        return Impl.SERDES;
    }

    /**
     * The raw Avro protocol. The full payload is the normal binary encoding of the Avro data.
     *
     * @return the raw Avro protocol
     * @see <a href="https://avro.apache.org/">Avro</a>
     */
    static Protocol raw() {
        return Impl.RAW;
    }

    enum Impl implements Protocol {
        RAW, SERDES;
    }
}
