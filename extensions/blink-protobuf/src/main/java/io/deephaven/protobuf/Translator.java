package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.Objects;

/**
 * Workaround for <a href="https://github.com/confluentinc/schema-registry/issues/2708">https://github.com/confluentinc/schema-registry/issues/2708</a>
 */
class Translator<T extends Message> implements SingleValuedMessageParser {
    public static void what(Descriptor badDescriptor, SingleValuedMessageParser parser) {


    }

    private final Descriptor descriptor;
    private final SingleValuedMessageParser delegate;
    private final Parser<T> parser;

    private Translator(Descriptor descriptor, SingleValuedMessageParser delegate, Parser<T> parser) {
        this.descriptor = Objects.requireNonNull(descriptor);
        this.delegate = Objects.requireNonNull(delegate);
        this.parser = Objects.requireNonNull(parser);
    }

    @Override
    public Descriptor descriptor() {
        return descriptor;
    }

    @Override
    public TypedFunction<Message> parser(ProtobufOptions options) {
        return delegate.parser(options).mapInput(this::mapInput);
    }

    private T mapInput(Message message) {
        if (!(message instanceof DynamicMessage)) {
            throw new RuntimeException("Expected dynamic message...");
        }
        try {
            return parser.parseFrom(message.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

//    private static <T extends Message> T translate(Class<T> clazz, Parser<T> parser, Message message)
//            throws InvalidProtocolBufferException {
//        if (clazz.isInstance(message)) {
//            return clazz.cast(message);
//        }
//        // This is ugly.
//        return parser.parseFrom(message.toByteString());
//    }
}
