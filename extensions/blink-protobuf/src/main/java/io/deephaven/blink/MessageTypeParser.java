package io.deephaven.blink;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public interface MessageTypeParser {

    static List<MessageTypeParser> builtin() {
        return Protobuf.builtinParsers();
    }

    static Iterable<MessageTypeParser> serviceLoaders() {
        return ServiceLoader.load(MessageTypeParser.class);
    }

    static Map<String, MessageTypeParser> defaults() {
        Map<String, MessageTypeParser> map = new HashMap<>();
        for (MessageTypeParser parser : builtin()) {
            map.put(parser.fullName(), parser);
        }
        for (MessageTypeParser parser : serviceLoaders()) {
            map.put(parser.fullName(), parser);
        }
        return map;
    }

    String fullName();

    TypedFunction<Message> parse(FieldDescriptor fd, ProtobufOptions options);

    TypedFunction<Message> parseRepeated(FieldDescriptor fd, ProtobufOptions options);
}
