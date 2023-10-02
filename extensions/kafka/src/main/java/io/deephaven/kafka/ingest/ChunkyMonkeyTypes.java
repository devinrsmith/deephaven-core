package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;

public class ChunkyMonkeyTypes {

    public static ChunkType of(Type<?> type) {
        return type.walk(Impl.INSTANCE);
    }

    public static ChunkType of(PrimitiveType<?> type) {
        return type.walk((PrimitiveType.Visitor<ChunkType>) Impl.INSTANCE);
    }

    public static ChunkType of(GenericType<?> type) {
        return type.walk((GenericType.Visitor<ChunkType>) Impl.INSTANCE);
    }

    private enum Impl implements Type.Visitor<ChunkType>, PrimitiveType.Visitor<ChunkType>, GenericType.Visitor<ChunkType> {
        INSTANCE;

        @Override
        public ChunkType visit(PrimitiveType<?> primitiveType) {
            return of(primitiveType);
        }

        @Override
        public ChunkType visit(GenericType<?> genericType) {
            return of(genericType);
        }

        @Override
        public ChunkType visit(BooleanType booleanType) {
            // todo: should this be ChunkType.Boolean?
            return ChunkType.Byte;
        }

        @Override
        public ChunkType visit(ByteType byteType) {
            return ChunkType.Byte;
        }

        @Override
        public ChunkType visit(CharType charType) {
            return ChunkType.Char;
        }

        @Override
        public ChunkType visit(ShortType shortType) {
            return ChunkType.Short;
        }

        @Override
        public ChunkType visit(IntType intType) {
            return ChunkType.Int;
        }

        @Override
        public ChunkType visit(LongType longType) {
            return ChunkType.Long;
        }

        @Override
        public ChunkType visit(FloatType floatType) {
            return ChunkType.Float;
        }

        @Override
        public ChunkType visit(DoubleType doubleType) {
            return ChunkType.Double;
        }


        @Override
        public ChunkType visit(BoxedType<?> boxedType) {
            // todo: is this the same as primitive types?
            // maybe w/ the exception of boolean?
            return of(boxedType.primitiveType());
        }

        @Override
        public ChunkType visit(StringType stringType) {
            return ChunkType.Object;
        }

        @Override
        public ChunkType visit(InstantType instantType) {
            // special case for Instant, we expect underlying chunk to be epochNanos.
            return ChunkType.Long;
        }

        @Override
        public ChunkType visit(ArrayType<?, ?> arrayType) {
            return ChunkType.Object;
        }

        @Override
        public ChunkType visit(CustomType<?> customType) {
            return ChunkType.Object;
        }
    }
}
