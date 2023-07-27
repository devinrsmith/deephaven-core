package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

interface SingleValuedMessageParser {

    static List<SingleValuedMessageParser> builtin() {
        return Builtin.parsers();
    }

    static Iterable<SingleValuedMessageParser> serviceLoaders() {
        return ServiceLoader.load(SingleValuedMessageParser.class);
    }

    static Map<Descriptor, SingleValuedMessageParser> defaults() {
        Map<Descriptor, SingleValuedMessageParser> map = new HashMap<>();
        for (SingleValuedMessageParser parser : builtin()) {
            map.put(parser.descriptor(), parser);
        }
        for (SingleValuedMessageParser parser : serviceLoaders()) {
            map.put(parser.descriptor(), parser);
        }
        return map;
    }

    Descriptor descriptor();

    TypedFunction<Message> messageParser(ProtobufOptions options);
}
