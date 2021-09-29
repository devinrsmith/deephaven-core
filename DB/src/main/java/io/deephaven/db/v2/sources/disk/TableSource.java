package io.deephaven.db.v2.sources.disk;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults.ForInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.lmdbjava.Env;

import java.nio.ByteBuffer;

public class TableSource {

    //private final Env<ByteBuffer> env;

    abstract class IntColumnSource extends AbstractColumnSource<Integer> implements ForInt {


        public IntColumnSource(@NotNull Class<Integer> type) {
            super(type);
        }

        public IntColumnSource(@NotNull Class<Integer> type, @Nullable Class<?> elementType) {
            super(type, elementType);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        @Override
        public Integer get(long index) {
            return null;
        }

        @Override
        public Boolean getBoolean(long index) {
            return null;
        }

        @Override
        public byte getByte(long index) {
            return 0;
        }

        @Override
        public char getChar(long index) {
            return 0;
        }

        @Override
        public double getDouble(long index) {
            return 0;
        }

        @Override
        public float getFloat(long index) {
            return 0;
        }

        @Override
        public int getInt(long index) {
            return 0;
        }

        @Override
        public long getLong(long index) {
            return 0;
        }

        @Override
        public short getShort(long index) {
            return 0;
        }

        @Override
        public Integer getPrev(long index) {
            return null;
        }

        @Override
        public Boolean getPrevBoolean(long index) {
            return null;
        }

        @Override
        public byte getPrevByte(long index) {
            return 0;
        }

        @Override
        public char getPrevChar(long index) {
            return 0;
        }

        @Override
        public double getPrevDouble(long index) {
            return 0;
        }

        @Override
        public float getPrevFloat(long index) {
            return 0;
        }

        @Override
        public int getPrevInt(long index) {
            return 0;
        }

        @Override
        public long getPrevLong(long index) {
            return 0;
        }

        @Override
        public short getPrevShort(long index) {
            return 0;
        }
    }
}
