/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ObjectFunction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

class BigDecimalFunction implements ObjectFunction<Object, BigDecimal> {
    private static final CustomType<BigDecimal> BIG_DECIMAL_TYPE = Type.ofCustom(BigDecimal.class);

    private final int scale;

    public BigDecimalFunction(final int precisionUnused, final int scale) {
        this.scale = scale;
    }

    @Override
    public GenericType<BigDecimal> returnType() {
        return BIG_DECIMAL_TYPE;
    }

    @Override
    public BigDecimal apply(Object bytesObj) {
        if (bytesObj == null) {
            return null;
        }
        if (bytesObj instanceof byte[]) {
            final byte[] bytes = (byte[]) bytesObj;
            final BigInteger bi = new BigInteger(bytes);
            return new BigDecimal(bi, scale);
        }
        if (bytesObj instanceof ByteBuffer) {
            final ByteBuffer bb = (ByteBuffer) bytesObj;
            final BigInteger bi;
            if (bb.hasArray()) {
                bi = new BigInteger(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
            } else {
                final byte[] bytes = new byte[bb.remaining()];
                bb.get(bytes);
                bi = new BigInteger(bytes);
            }
            return new BigDecimal(bi, scale);
        }
        throw new IllegalStateException(
                "Object of type " + bytesObj.getClass().getName() + " not recognized for decimal type backing");
    }
}
