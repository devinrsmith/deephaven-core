package io.deephaven.function.reflect;

import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

public final class MethodGetter<T> implements Type.Visitor<TypedFunction<T>>, PrimitiveType.Visitor<TypedFunction<T>> {
    public static TypedFunction<?> of(Method method) {
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException();
        }
        return Type.find(method.getReturnType()).walk(new MethodGetter<>(method));
    }

    public static <T> TypedFunction<T> of(Method method, Class<T> clazz) {
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException();
        }
        if (!clazz.equals(method.getParameterTypes()[0])) {
            throw new IllegalArgumentException();
        }
        return Type.find(method.getReturnType()).walk(new MethodGetter<>(method));
    }

    public static <T> ToIntFunction<T> ofInt(Method method) {
        // todo: check arg against <T>
        return new IntMethod<>(method);
    }

    public static <T> ToLongFunction<T> ofLong(Method method) {
        // todo: check arg against <T>

        return new LongMethod<>(method);
    }

    public static <T, R> ToObjectFunction<T, R> ofGeneric(Method method, GenericType<R> genericType) {
        // todo: check arg against <T>
        return new GenericMethod<>(method, genericType);
    }

    private static abstract class MethodBase<T> implements TypedFunction<T> {

        protected final Method method;

        public MethodBase(Method method) {
            if (method.getParameterCount() != 1) {
                throw new IllegalArgumentException();
            }
            this.method = Objects.requireNonNull(method);
        }
    }

    private static class IntMethod<T> extends MethodBase<T> implements ToIntFunction<T> {
        private IntMethod(Method method) {
            super(method);
            if (!int.class.equals(method.getReturnType())) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public int applyAsInt(T value) {
            try {
                return (int) method.invoke(value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class LongMethod<T> extends MethodBase<T> implements ToLongFunction<T> {
        private LongMethod(Method method) {
            super(method);
            if (!int.class.equals(method.getReturnType())) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public long applyAsLong(T value) {
            try {
                return (long) method.invoke(value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class GenericMethod<T, R> extends MethodBase<T> implements ToObjectFunction<T, R> {
        private final GenericType<R> genericType;

        public GenericMethod(Method method, GenericType<R> genericType) {
            super(method);
            this.genericType = Objects.requireNonNull(genericType);
        }

        @Override
        public GenericType<R> returnType() {
            return genericType;
        }

        @Override
        public R apply(T value) {
            try {
                //noinspection unchecked
                return (R) method.invoke(value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Method method;

    private MethodGetter(Method method) {
        this.method = Objects.requireNonNull(method);
    }

    @Override
    public TypedFunction<T> visit(PrimitiveType<?> primitiveType) {
        return primitiveType.walk((PrimitiveType.Visitor<TypedFunction<T>>) this);
    }

    @Override
    public TypedFunction<T> visit(GenericType<?> genericType) {
        return generic(genericType);
    }

    private <R> TypedFunction<T> generic(GenericType<R> genericType) {
        return ToObjectFunction.of(this::invokeUnsafe, genericType);
    }

    @Override
    public TypedFunction<T> visit(BooleanType booleanType) {
        return (ToBooleanFunction<T>) this::invokeBoolean;
    }

    @Override
    public TypedFunction<T> visit(ByteType byteType) {
        return (ToByteFunction<T>) this::invokeByte;
    }

    @Override
    public TypedFunction<T> visit(CharType charType) {
        return (ToCharFunction<T>) this::invokeChar;
    }

    @Override
    public TypedFunction<T> visit(ShortType shortType) {
        return (ToShortFunction<T>) this::invokeShort;
    }

    @Override
    public TypedFunction<T> visit(IntType intType) {
        return ofInt(method);
    }

    @Override
    public TypedFunction<T> visit(LongType longType) {
        return (ToLongFunction<T>) this::invokeLong;
    }

    @Override
    public TypedFunction<T> visit(FloatType floatType) {
        return (ToFloatFunction<T>) this::invokeFloat;
    }

    @Override
    public TypedFunction<T> visit(DoubleType doubleType) {
        return (ToDoubleFunction<T>) this::invokeDouble;
    }

    private <R> R invokeUnsafe(T obj) {
        try {
            //noinspection unchecked
            return (R) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean invokeBoolean(T obj) {
        try {
            return (boolean) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private char invokeChar(T obj) {
        try {
            return (char) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private byte invokeByte(T obj) {
        try {
            return (byte) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private short invokeShort(T obj) {
        try {
            return (short) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private int invokeInt(T obj) {
        try {
            return (int) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private long invokeLong(T obj) {
        try {
            return (long) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private float invokeFloat(T obj) {
        try {
            return (float) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private double invokeDouble(T obj) {
        try {
            return (double) method.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
