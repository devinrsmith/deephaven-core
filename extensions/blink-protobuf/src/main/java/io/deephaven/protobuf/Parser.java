package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public interface Parser {

    static List<Parser> builtin() {
        return Protobuf.builtinParsers();
    }

    static Iterable<Parser> serviceLoaders() {
        return ServiceLoader.load(Parser.class);
    }

    static Map<String, Parser> defaults() {
        Map<String, Parser> map = new HashMap<>();
        for (Parser parser : builtin()) {
            map.put(parser.fullName(), parser);
        }
        for (Parser parser : serviceLoaders()) {
            map.put(parser.fullName(), parser);
        }
        return map;
    }

    String fullName();

    TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options);

    TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options);
}
