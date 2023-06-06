package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public interface SingleValuedMessageParser {

    static List<SingleValuedMessageParser> builtin() {
        return Protobuf.builtinParsers();
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

    default GenericMessageParser asGenericMessageParser() {
        return new GenericMessageParser() {
            @Override
            public String fullName() {
                return SingleValuedMessageParser.this.fullName();
            }

            @Override
            public Map<List<String>, TypedFunction<Message>> parser(ProtobufOptions options) {
                return Map.of(List.of(), SingleValuedMessageParser.this.parser(options));
            }
        };
    }
}
