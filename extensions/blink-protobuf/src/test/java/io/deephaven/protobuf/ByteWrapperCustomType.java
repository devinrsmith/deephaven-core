package io.deephaven.protobuf;

import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.protobuf.test.ByteWrapper;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

@AutoService(SingleValuedMessageParser.class)
public class ByteWrapperCustomType implements SingleValuedMessageParser {
    public ByteWrapperCustomType() {}

    @Override
    public Descriptor canonicalDescriptor() {
        return ByteWrapper.getDescriptor();
    }

    @Override
    public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufOptions options) {
        final FieldDescriptor field = descriptor.findFieldByNumber(ByteWrapper.VALUE_FIELD_NUMBER);
        return (ByteFunction<Message>) value -> (byte) (int) value.getField(field);
    }
}