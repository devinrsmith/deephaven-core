package io.deephaven.protobuf;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.charset.StandardCharsets;

public class RuntimeSchema {

    public static void what() throws InvalidProtocolBufferException, DescriptorValidationException {
        final FileDescriptorSet set = FileDescriptorSet.parseFrom("".getBytes(StandardCharsets.UTF_8));

        FileDescriptor.buildFrom(FileDescriptorProto.parseFrom("todo".getBytes(StandardCharsets.UTF_8)), new FileDescriptor[] { });

        for (FileDescriptorProto fdp : set.getFileList()) {

            FileDescriptor.buildFrom(fdp, new FileDescriptor[] { });
        }



    }
}
