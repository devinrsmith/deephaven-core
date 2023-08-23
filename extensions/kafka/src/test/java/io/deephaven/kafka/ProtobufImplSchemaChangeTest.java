package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.kafka.test.BoolV1;
import io.deephaven.kafka.test.BoolV2;
import io.deephaven.kafka.test.MyMessageV1;
import io.deephaven.kafka.test.MyMessageV2;
import io.deephaven.kafka.test.MyMessageV3;
import io.deephaven.kafka.test.MyMessageV3.MyMessage.FirstAndLast;
import io.deephaven.kafka.test.MyMessageV4;
import io.deephaven.kafka.test.MyMessageV5;
import io.deephaven.kafka.test.RenameV1;
import io.deephaven.kafka.test.RenameV2;
import io.deephaven.protobuf.ProtobufFunction;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;


/**
 * See notes in TESTING.md for adding new tests here.
 */
public class ProtobufImplSchemaChangeTest {

    @Test
    public void myMessageV1toV2() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV1.MyMessage.getDescriptor());
        assertThat(functions.functions()).hasSize(1);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
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
        assertThat(functions.functions()).hasSize(2);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        final IntFunction<Message> ageFunction = (IntFunction<Message>) get(functions, "age");
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
        assertThat(functions.functions()).hasSize(2);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        final IntFunction<Message> ageFunction = (IntFunction<Message>) get(functions, "age");
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
        assertThat(functions.functions()).hasSize(4);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        final ObjectFunction<Message, String> firstNameFunction =
                (ObjectFunction<Message, String>) get(functions, "first_and_last", "first_name");
        final ObjectFunction<Message, String> lastNameFunction =
                (ObjectFunction<Message, String>) get(functions, "first_and_last", "last_name");
        final IntFunction<Message> ageFunction = (IntFunction<Message>) get(functions, "age");
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
        assertThat(functions.functions()).hasSize(4);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        final IntFunction<Message> ageFunction = (IntFunction<Message>) get(functions, "age");
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
        assertThat(functions.functions()).hasSize(5);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        final IntFunction<Message> ageFunction = (IntFunction<Message>) get(functions, "age");
        final FloatFunction<Message> agefFunction = (FloatFunction<Message>) get(functions, "agef");
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

    @Test
    public void myMessageV4toV5() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV4.MyMessage.getDescriptor());
        assertThat(functions.functions()).hasSize(5);
        final ObjectFunction<Message, String> lastNameFunction =
                (ObjectFunction<Message, String>) get(functions, "first_and_last", "last_name");
        {
            final MyMessageV5.MyMessage v5 = MyMessageV5.MyMessage.getDefaultInstance();
            try {
                lastNameFunction.apply(v5);
                failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
            } catch (UncheckedDeephavenException e) {
                assertThat(e).hasMessage(
                        "Incompatible schema change for [first_and_last, last_name], originalType=io.deephaven.qst.type.StringType, newType=io.deephaven.qst.type.LongType");
            }
        }
    }

    @Test
    public void myMessageV5toV4() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(MyMessageV5.MyMessage.getDescriptor());
        assertThat(functions.functions()).hasSize(5);
        final LongFunction<Message> lastNameFunction =
                (LongFunction<Message>) get(functions, "first_and_last", "last_name");
        {
            final MyMessageV4.MyMessage v4 = MyMessageV4.MyMessage.getDefaultInstance();
            try {
                lastNameFunction.applyAsLong(v4);
                failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
            } catch (UncheckedDeephavenException e) {
                assertThat(e).hasMessage(
                        "Incompatible schema change for [first_and_last, last_name], originalType=io.deephaven.qst.type.LongType, newType=io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    public void renameV1toV2() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(RenameV1.Rename.getDescriptor());
        assertThat(functions.functions()).hasSize(1);
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        {
            final RenameV1.Rename v1 = RenameV1.Rename.newBuilder().setName("v1").build();
            assertThat(nameFunction.apply(v1)).isEqualTo("v1");
        }
        {
            final RenameV2.Rename v2 = RenameV2.Rename.newBuilder().setNameOld("v2").setName("v2-new-name").build();
            assertThat(nameFunction.apply(v2)).isEqualTo("v2");
        }
    }

    @Test
    public void renameV2toV1() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(RenameV2.Rename.getDescriptor());
        assertThat(functions.functions()).hasSize(2);
        final ObjectFunction<Message, String> nameOldFunction =
                (ObjectFunction<Message, String>) get(functions, "name_old");
        final ObjectFunction<Message, String> nameFunction = (ObjectFunction<Message, String>) get(functions, "name");
        {
            final RenameV2.Rename v2 = RenameV2.Rename.newBuilder().setNameOld("v2").setName("v2-new-name").build();
            assertThat(nameOldFunction.apply(v2)).isEqualTo("v2");
            assertThat(nameFunction.apply(v2)).isEqualTo("v2-new-name");
        }
        {
            final RenameV1.Rename v1 = RenameV1.Rename.newBuilder().setName("v1").build();
            assertThat(nameOldFunction.apply(v1)).isEqualTo("v1");
            assertThat(nameFunction.apply(v1)).isNull();
        }
    }

    @Test
    public void boolV1toV2() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(BoolV1.MyBool.getDescriptor());
        assertThat(functions.functions()).hasSize(1);
        // Note: it's important that this is parsed as ObjectFunction<Message, Boolean> instead of
        // BooleanFunction<Message> because we need to be able to handle schema changes which might remove the field
        final ObjectFunction<Message, Boolean> myBoolFunction =
                (ObjectFunction<Message, Boolean>) get(functions, "my_bool");
        {
            final BoolV1.MyBool v1 = BoolV1.MyBool.newBuilder().setMyBool(true).build();
            assertThat(myBoolFunction.apply(v1)).isTrue();
        }
        {
            final BoolV1.MyBool v1 = BoolV1.MyBool.newBuilder().setMyBool(false).build();
            assertThat(myBoolFunction.apply(v1)).isFalse();
        }
        {
            final BoolV2.MyBool v2 = BoolV2.MyBool.getDefaultInstance();
            assertThat(myBoolFunction.apply(v2)).isNull();
        }
    }

    @Test
    public void boolV2IsEmpty() {
        final ProtobufFunctions functions = schemaChangeAwareFunctions(BoolV2.MyBool.getDescriptor());
        assertThat(functions.functions()).isEmpty();
    }

    private static ProtobufFunctions schemaChangeAwareFunctions(Descriptor descriptor) {
        return ProtobufImpl.schemaChangeAwareFunctions(descriptor, ProtobufDescriptorParserOptions.defaults());
    }

    private static TypedFunction<Message> get(ProtobufFunctions functions, String... namePath) {
        return functions.find(Arrays.asList(namePath)).map(ProtobufFunction::function).get();
    }
}
