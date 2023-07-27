package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public interface SingleValuedMessageParser {

    static List<SingleValuedMessageParser> builtin() {
        return Builtin.parsers();
    }

    static Iterable<SingleValuedMessageParser> serviceLoaders() {
        return ServiceLoader.load(SingleValuedMessageParser.class);
    }

    static Map<String, SingleValuedMessageParser> defaults() {
        Map<String, SingleValuedMessageParser> map = new HashMap<>();
        for (SingleValuedMessageParser parser : builtin()) {
            map.put(parser.fullName(), parser);
        }
        for (SingleValuedMessageParser parser : serviceLoaders()) {
            map.put(parser.fullName(), parser);
        }
        return map;
    }

    String fullName();

    TypedFunction<Message> parser(ProtobufOptions options);
}
