package io.deephaven.avro;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

class AvroParsingStateDeser implements Deserializer<AvroParsingState> {

    @Override
    public AvroParsingState deserialize(String topic, byte[] data) {
        // todo
        return new AvroParsingState(data, new MutableInt(5));
    }
}
