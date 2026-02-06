package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Objects;

final class AvroDeserializers {

    private static class AvroRawDeserializer<D> implements Deserializer<D> {
        private final DecoderFactory decoderFactory;
        private final DatumReader<D> reader;

        private AvroRawDeserializer(DecoderFactory decoderFactory, GenericDatumReader<D> reader) {
            this.decoderFactory = Objects.requireNonNull(decoderFactory);
            this.reader = Objects.requireNonNull(reader);
        }

        @Override
        public D deserialize(String topic, byte[] data) {
            final BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
            try {
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }
    }

    /**
     * Our getProcessor relies on a specific {@link Schema}; we need to ensure that Kafka layer adapts the on-wire
     * writer's schema to our reader's schema.
     */
    private static class KafkaAvroDeserializerWithReaderSchema extends KafkaAvroDeserializer {
        private final Schema schema;

        public KafkaAvroDeserializerWithReaderSchema(SchemaRegistryClient client, Schema schema) {
            super(Objects.requireNonNull(client));
            this.schema = Objects.requireNonNull(schema);
        }

        @Override
        public java.lang.Object deserialize(String topic, byte[] bytes) {
            return super.deserialize(topic, bytes, schema);
        }

        @Override
        public java.lang.Object deserialize(String topic, Headers headers, byte[] bytes) {
            return super.deserialize(topic, headers, bytes, schema);
        }
    }
}
