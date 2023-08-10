package io.deephaven.protobuf;

import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;

public class CommonFunctions {

    private static final IntFunction<Message> SERIALIZED_SIZE = Message::getSerializedSize;
    private static final ObjectFunction<Message, UnknownFieldSet> UNKNOWN_FIELD_SET =
            ObjectFunction.of(Message::getUnknownFields, Type.ofCustom(UnknownFieldSet.class));
    private static final ObjectFunction<Message, Message> MESSAGE_IDENTITY =
            ObjectFunction.identity(Type.ofCustom(Message.class));

    public static IntFunction<Message> serializedSize() {
        return SERIALIZED_SIZE;
    }

    public static ObjectFunction<Message, UnknownFieldSet> unknownFieldSet() {
        return UNKNOWN_FIELD_SET;
    }

    public static ObjectFunction<Message, Message> messageIdentity() {
        return MESSAGE_IDENTITY;
    }
}
