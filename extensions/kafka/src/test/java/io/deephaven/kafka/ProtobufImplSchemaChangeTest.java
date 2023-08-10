package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.kafka.test.MyMessageV1;
import io.deephaven.kafka.test.MyMessageV2;
import io.deephaven.kafka.test.MyMessageV3;
import io.deephaven.kafka.test.MyMessageV3.MyMessage.FirstAndLast;
import io.deephaven.kafka.test.MyMessageV4;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufOptions;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * See notes in TESTING.md for adding new tests here.
 */
public class ProtobufImplSchemaChangeTest {

    @Test
    public void myMessageV1toV2() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV1.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(1);
        final ObjectFunction<Message, String> nameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("name")));
        {
            final MyMessageV1.MyMessage v1 = MyMessageV1.MyMessage.newBuilder().setName("v1").build();
            assertThat(nameFunction.apply(v1)).isEqualTo("v1");
        }
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setName("v2").setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEqualTo("v2");
        }
    }

    @Test
    public void myMessageV2toV1() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV2.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(2);
        final ObjectFunction<Message, String> nameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("name")));
        final IntFunction<Message> ageFunction = IntFunction.cast(functions.columns().get(List.of("age")));
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setName("v2").setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEqualTo("v2");
            assertThat(ageFunction.applyAsInt(v2)).isEqualTo(2);
        }
        {
            final MyMessageV1.MyMessage v1 = MyMessageV1.MyMessage.newBuilder().setName("v1").build();
            assertThat(nameFunction.apply(v1)).isEqualTo("v1");
            assertThat(ageFunction.applyAsInt(v1)).isEqualTo(QueryConstants.NULL_INT);
        }
    }

    @Test
    public void myMessageV2toV3() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV2.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(2);
        final ObjectFunction<Message, String> nameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("name")));
        final IntFunction<Message> ageFunction = IntFunction.cast(functions.columns().get(List.of("age")));
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setName("v2").setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEqualTo("v2");
            assertThat(ageFunction.applyAsInt(v2)).isEqualTo(2);
        }
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEmpty();
            assertThat(ageFunction.applyAsInt(v2)).isEqualTo(2);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").setAge(3).build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder()
                    .setFirstAndLast(FirstAndLast.newBuilder().setFirstName("First").setLastName("Last").build())
                    .setAge(3)
                    .build();
            assertThat(nameFunction.apply(v3)).isNull();
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setAge(3).build();
            assertThat(nameFunction.apply(v3)).isNull();
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
    }

    @Test
    public void myMessageV3toV2() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV3.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(4);
        final ObjectFunction<Message, String> nameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("name")));
        final ObjectFunction<Message, String> firstNameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("first_and_last", "first_name")));
        final ObjectFunction<Message, String> lastNameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("first_and_last", "last_name")));
        final IntFunction<Message> ageFunction = IntFunction.cast(functions.columns().get(List.of("age")));
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").setAge(3).build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(firstNameFunction.apply(v3)).isNull();
            assertThat(lastNameFunction.apply(v3)).isNull();
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder()
                    .setFirstAndLast(FirstAndLast.newBuilder().setFirstName("First").setLastName("Last").build())
                    .setAge(3)
                    .build();
            assertThat(nameFunction.apply(v3)).isNull();
            assertThat(firstNameFunction.apply(v3)).isEqualTo("First");
            assertThat(lastNameFunction.apply(v3)).isEqualTo("Last");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setAge(3).build();
            assertThat(nameFunction.apply(v3)).isNull();
            assertThat(firstNameFunction.apply(v3)).isNull();
            assertThat(lastNameFunction.apply(v3)).isNull();
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setName("v2").setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEqualTo("v2");
            assertThat(firstNameFunction.apply(v2)).isNull();
            assertThat(lastNameFunction.apply(v2)).isNull();
            assertThat(ageFunction.applyAsInt(v2)).isEqualTo(2);
        }
        {
            final MyMessageV2.MyMessage v2 = MyMessageV2.MyMessage.newBuilder().setAge(2).build();
            assertThat(nameFunction.apply(v2)).isEmpty();
            assertThat(firstNameFunction.apply(v2)).isNull();
            assertThat(lastNameFunction.apply(v2)).isNull();
            assertThat(ageFunction.applyAsInt(v2)).isEqualTo(2);
        }
    }

    @Test
    public void myMessageV3toV4() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV3.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(4);
        final ObjectFunction<Message, String> nameFunction =
                ObjectFunction.cast(functions.columns().get(List.of("name")));
        final IntFunction<Message> ageFunction = IntFunction.cast(functions.columns().get(List.of("age")));
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").setAge(3).build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(0);
        }
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").setAge(4).build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(4);
        }
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").setAgef(4.4f).build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(QueryConstants.NULL_INT);
        }
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(QueryConstants.NULL_INT);
        }
    }

    @Test
    public void myMessageV4toV3() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV4.MyMessage.getDescriptor());
        assertThat(functions.columns()).hasSize(5);
        final ObjectFunction<Message, String> nameFunction = ObjectFunction.cast(functions.columns().get(List.of("name")));
        final IntFunction<Message> ageFunction = IntFunction.cast(functions.columns().get(List.of("age")));
        final FloatFunction<Message> agefFunction = FloatFunction.cast(functions.columns().get(List.of("agef")));
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").setAge(4).build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(4);
            assertThat(agefFunction.applyAsFloat(v4)).isEqualTo(QueryConstants.NULL_FLOAT);
        }
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").setAgef(4.4f).build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(QueryConstants.NULL_INT);
            assertThat(agefFunction.applyAsFloat(v4)).isEqualTo(4.4f);
        }
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.newBuilder().setName("v4").build();
            assertThat(nameFunction.apply(v4)).isEqualTo("v4");
            assertThat(ageFunction.applyAsInt(v4)).isEqualTo(QueryConstants.NULL_INT);
            assertThat(agefFunction.applyAsFloat(v4)).isEqualTo(QueryConstants.NULL_FLOAT);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").setAge(3).build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(3);
            assertThat(agefFunction.applyAsFloat(v3)).isEqualTo(QueryConstants.NULL_FLOAT);
        }
        {
            final MyMessageV3.MyMessage v3 = MyMessageV3.MyMessage.newBuilder().setName("v3").build();
            assertThat(nameFunction.apply(v3)).isEqualTo("v3");
            assertThat(ageFunction.applyAsInt(v3)).isEqualTo(0);
            assertThat(agefFunction.applyAsFloat(v3)).isEqualTo(QueryConstants.NULL_FLOAT);
        }
    }

    private static ProtobufFunctions schemaChangeAwareFunctions(Descriptor descriptor) {
        return ProtobufImpl.schemaChangeAwareFunctions(descriptor, ProtobufOptions.defaults());
    }
}
