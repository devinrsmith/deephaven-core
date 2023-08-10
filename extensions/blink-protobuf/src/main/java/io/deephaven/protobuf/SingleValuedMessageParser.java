package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.ArrayList;
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

    static List<SingleValuedMessageParser> defaults() {
        final List<SingleValuedMessageParser> out = new ArrayList<>(builtin());
        for (SingleValuedMessageParser parser : serviceLoaders()) {
            out.add(parser);
        }
        return out;
    }

    Descriptor canonicalDescriptor();

    TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufOptions options);
}
