package io.deephaven.protobuf;

import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.protobuf.test.ByteWrapper;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

@AutoService(SingleValuedMessageParser.class)
public class ByteWrapperCustomType implements SingleValuedMessageParser {
    private static final CustomType<ByteWrapper> IN_TYPE = Type.ofCustom(ByteWrapper.class);

    public ByteWrapperCustomType() {}

    @Override
    public Descriptor descriptor() {
        return ByteWrapper.getDescriptor();
    }

    @Override
    public TypedFunction<Message> messageParser(ProtobufOptions options) {
        return ObjectFunction.<Message, ByteWrapper>cast(IN_TYPE).mapByte(x -> (byte) x.getValue());
    }
}
