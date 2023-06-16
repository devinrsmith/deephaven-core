package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.List;
import java.util.Map;

public interface GenericMessageParser {

    String fullName();

    Map<List<String>, TypedFunction<Message>> parse(FieldDescriptor fd, ProtobufOptions options);

    Map<List<String>, TypedFunction<Message>> parseRepeated(FieldDescriptor fd, ProtobufOptions options);
}
