//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.reflect.MethodGetter;
import io.deephaven.processor.sink.appender.Appender;
import io.deephaven.processor.sink.appender.BooleanAppender;
import io.deephaven.processor.sink.appender.ByteAppender;
import io.deephaven.processor.sink.appender.CharAppender;
import io.deephaven.processor.sink.appender.DoubleAppender;
import io.deephaven.processor.sink.appender.FloatAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.processor.sink.appender.ShortAppender;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class Proxy {

    // preloaded Method objects for the methods in java.lang.Object
    private static Method HASHCODE_METHOD;

    private static Method EQUALS_METHOD;

    private static Method TOSTRING_METHOD;

    static {
        try {
            HASHCODE_METHOD = Object.class.getMethod("hashCode");
            EQUALS_METHOD = Object.class.getMethod("equals", Object.class);
            TOSTRING_METHOD = Object.class.getMethod("toString");
        } catch (NoSuchMethodException e) {
            throw new NoSuchMethodError(e.getMessage());
        }
    }

    @Target({ElementType.METHOD, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Info {
        int index();
    }

    public static <T> StreamingTarget<T> streamingTarget(Class<T> clazz) {

        List<Method> methods = new ArrayList<>();
        List<Type<?>> types = new ArrayList<>();


        Method advanceAllMethod = null;
        Method ensureRemainingCapacityMethod = null;



        for (Method method : clazz.getMethods()) {
            if (Objects.equals(method, EQUALS_METHOD) || Objects.equals(method, HASHCODE_METHOD)
                    || Objects.equals(method, TOSTRING_METHOD)) {
                continue;
            }
            if (void.class.equals(method.getReturnType()) && method.getParameterCount() == 1
                    && long.class.equals(method.getParameterTypes()[0])
                    && "ensureRemainingCapacity".equals(method.getName())) {
                if (ensureRemainingCapacityMethod != null) {
                    throw new IllegalArgumentException();
                }
                ensureRemainingCapacityMethod = method;
                continue;
            }
            if (void.class.equals(method.getReturnType()) && method.getParameterCount() == 0
                    && ("advance".equals(method.getName()) || "advanceAll".equals(method.getName()))) {
                if (advanceAllMethod != null) {
                    throw new IllegalArgumentException();
                }
                advanceAllMethod = method;
                continue;
            }
            if (void.class.equals(method.getReturnType()) && method.getParameterCount() == 1) {
                // final Annotation[] annotations = method.getAnnotations();
                // final java.lang.reflect.Type type = method.getGenericParameterTypes()[0];
                final Type<?> type = Type.find(method.getParameterTypes()[0]);
                // todo: handle annotations
                // method.getAnnotatedParameterTypes()[0]
                methods.add(method);
                types.add(type);
                continue;
            }
            throw new IllegalStateException("Unexpected method " + method);
        }
        return new StreamingTarget<>(clazz, methods, types, advanceAllMethod, ensureRemainingCapacityMethod);
    }

    public static <T> ObjectTarget<T> objectTarget(Class<T> clazz) {
        List<Method> methods = new ArrayList<>();
        List<Type<?>> types = new ArrayList<>();
        for (Method method : clazz.getMethods()) {
            // todo: this is getting hashcode
            if (method.getParameterCount() == 0 && method.getReturnType().isPrimitive()
                    && method.getReturnType() != void.class) {
                methods.add(method);
                types.add(Type.find(method.getReturnType()));
            }
        }
        return new ObjectTarget<>(clazz, methods, types);
    }

    public static final class ObjectTarget<T> {

        private final Class<T> clazz;
        private final List<Method> methods;
        private final List<Type<?>> types;

        private ObjectTarget(Class<T> clazz, List<Method> methods, List<Type<?>> types) {
            this.clazz = Objects.requireNonNull(clazz);
            this.methods = List.copyOf(methods);
            this.types = List.copyOf(types);
        }

        public List<Method> methods() {
            return methods;
        }

        public List<Type<?>> types() {
            return types;
        }

        public Consumer<T> bind(Stream stream) {
            return new Impl(stream);
        }

        private class Impl implements Consumer<T> {
            private final Stream stream;
            private final List<Consumer<T>> appenders;

            public Impl(Stream stream) {
                this.stream = Objects.requireNonNull(stream);
                final int L = methods.size();
                if (stream.appenders().size() != L) {
                    throw new IllegalArgumentException();
                }
                appenders = new ArrayList<>(L);
                for (int i = 0; i < L; i++) {
                    final Appender appender = stream.appenders().get(i);
                    if (appender.type() != types.get(i)) {
                        throw new IllegalArgumentException();
                    }
                    try {
                        appenders.add(SetVisitor2.of(appender, methods.get(i)));
                    } catch (RuntimeException e) {
                        throw new RuntimeException("method " + methods.get(i), e);
                    }
                }
            }

            @Override
            public void accept(T t) {
                for (Consumer<T> appender : appenders) {
                    appender.accept(t);
                }
                stream.advanceAll();
            }
        }
    }


    public static final class StreamingTarget<T> {

        private final Class<T> clazz;
        private final List<Method> methods;
        private final List<Type<?>> types;
        private final Method advanceAllMethod;
        private final Method ensureRemainingCapacityMethod;

        private StreamingTarget(Class<T> clazz, List<Method> methods, List<Type<?>> types, Method advanceAllMethod,
                Method ensureRemainingCapacityMethod) {
            this.clazz = Objects.requireNonNull(clazz);
            this.methods = List.copyOf(methods);
            this.types = List.copyOf(types);
            this.advanceAllMethod = advanceAllMethod;
            this.ensureRemainingCapacityMethod = ensureRemainingCapacityMethod;
        }

        public T bind(Stream stream) {
            // noinspection unchecked
            return (T) java.lang.reflect.Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz},
                    new SetHandler(stream));
        }

        public List<Method> methods() {
            return methods;
        }

        public List<Type<?>> types() {
            return types;
        }

        private class SetHandler implements InvocationHandler {

            private final Stream stream;
            private final Map<Method, Consumer<Object>> appenders;

            public SetHandler(Stream stream) {
                this.stream = Objects.requireNonNull(stream);
                this.appenders = new HashMap<>();
                if (stream.appenders().size() != types().size()) {
                    throw new IllegalArgumentException();
                }
                final int L = stream.appenders().size();
                for (int i = 0; i < L; i++) {
                    final Appender appender = stream.appenders().get(i);
                    final Type<?> expectedType = types().get(i);
                    if (!expectedType.equals(appender.type())) {
                        throw new IllegalArgumentException();
                    }
                    final Method method = methods().get(i);
                    appenders.put(method, SetVisitor.of(appender));
                }
            }

            @Override
            public Object invoke(Object thisProxy, Method method, Object[] args) throws Throwable {
                final Consumer<Object> appender = appenders.get(method);
                if (appender != null) {
                    appender.accept(args[0]);
                    return null;
                }
                if (Objects.equals(method, advanceAllMethod)) {
                    stream.advanceAll();
                    return null;
                }
                if (Objects.equals(method, ensureRemainingCapacityMethod)) {
                    stream.ensureRemainingCapacity((long) args[0]);
                    return null;
                }
                if (Objects.equals(method, HASHCODE_METHOD)) {
                    return hashCode();
                }
                if (Objects.equals(method, EQUALS_METHOD)) {
                    final Object other = args[0];
                    if (thisProxy == other) {
                        return true;
                    }
                    if (!clazz.isInstance(other)) {
                        return false;
                    }
                    if (!java.lang.reflect.Proxy.isProxyClass(other.getClass())) {
                        return false;
                    }
                    final InvocationHandler otherHandler = java.lang.reflect.Proxy.getInvocationHandler(other);
                    return equals(otherHandler);
                }
                throw new NoSuchMethodError();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                SetHandler that = (SetHandler) o;
                return stream.equals(that.stream);
            }

            @Override
            public int hashCode() {
                return stream.hashCode() ^ 0xDEADBEEF;
            }
        }
    }

    private static class SetVisitor implements Type.Visitor<Consumer<Object>>, PrimitiveType.Visitor<Consumer<Object>>,
            GenericType.Visitor<Consumer<Object>>, BoxedType.Visitor<Consumer<Object>> {
        public static Consumer<Object> of(Appender appender) {
            return appender.type().walk(new SetVisitor(appender));
        }

        private final Appender appender;

        SetVisitor(Appender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public Consumer<Object> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(BooleanType booleanType) {
            final BooleanAppender app = BooleanAppender.get(appender);
            return arg -> app.set((boolean) arg);
        }

        @Override
        public Consumer<Object> visit(ByteType byteType) {
            final ByteAppender app = ByteAppender.get(appender);
            return arg -> app.set((byte) arg);
        }

        @Override
        public Consumer<Object> visit(CharType charType) {
            final CharAppender app = CharAppender.get(appender);
            return arg -> app.set((char) arg);
        }

        @Override
        public Consumer<Object> visit(ShortType shortType) {
            final ShortAppender app = ShortAppender.get(appender);
            return arg -> app.set((short) arg);
        }

        @Override
        public Consumer<Object> visit(IntType intType) {
            final IntAppender app = IntAppender.get(appender);
            return arg -> app.set((int) arg);
        }

        @Override
        public Consumer<Object> visit(LongType longType) {
            // todo: ability to ingest millis via long w/ annotation
            final LongAppender app = LongAppender.get(appender);
            return arg -> app.set((long) arg);
        }

        @Override
        public Consumer<Object> visit(FloatType floatType) {
            final FloatAppender app = FloatAppender.get(appender);
            return arg -> app.set((float) arg);
        }

        @Override
        public Consumer<Object> visit(DoubleType doubleType) {
            final DoubleAppender app = DoubleAppender.get(appender);
            return arg -> app.set((double) arg);
        }

        @Override
        public Consumer<Object> visit(BoxedBooleanType booleanType) {
            final BooleanAppender app = BooleanAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((boolean) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedByteType byteType) {
            final ByteAppender app = ByteAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((byte) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedCharType charType) {
            final CharAppender app = CharAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((char) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedShortType shortType) {
            final ShortAppender app = ShortAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((short) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedIntType intType) {
            final IntAppender app = IntAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((int) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedLongType longType) {
            final LongAppender app = LongAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((long) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedFloatType floatType) {
            final FloatAppender app = FloatAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((float) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedDoubleType doubleType) {
            final DoubleAppender app = DoubleAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set((double) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(StringType stringType) {
            return generic(stringType);
        }

        @Override
        public Consumer<Object> visit(InstantType instantType) {
            return generic(instantType);
        }

        @Override
        public Consumer<Object> visit(ArrayType<?, ?> arrayType) {
            return generic(arrayType);
        }

        @Override
        public Consumer<Object> visit(CustomType<?> customType) {
            return generic(customType);
        }

        private <T> Consumer<Object> generic(GenericType<T> type) {
            final ObjectAppender<T> app = ObjectAppender.get(appender, type);
            final Class<T> clazz = type.clazz();
            return arg -> {
                if (arg == null) {
                    app.setNull();
                } else {
                    app.set(clazz.cast(arg));
                }
            };
        }
    }

    private static class AppendVisitor
            implements Type.Visitor<Consumer<Object>>, PrimitiveType.Visitor<Consumer<Object>>,
            GenericType.Visitor<Consumer<Object>>, BoxedType.Visitor<Consumer<Object>> {
        public static Consumer<Object> of(Appender appender) {
            return appender.type().walk(new SetVisitor(appender));
        }

        private final Appender appender;

        AppendVisitor(Appender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public Consumer<Object> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Consumer<Object>>) this);
        }

        @Override
        public Consumer<Object> visit(BooleanType booleanType) {
            final BooleanAppender app = BooleanAppender.get(appender);
            return arg -> BooleanAppender.append(app, (boolean) arg);
        }

        @Override
        public Consumer<Object> visit(ByteType byteType) {
            final ByteAppender app = ByteAppender.get(appender);
            return arg -> ByteAppender.append(app, (byte) arg);
        }

        @Override
        public Consumer<Object> visit(CharType charType) {
            final CharAppender app = CharAppender.get(appender);
            return arg -> CharAppender.append(app, (char) arg);
        }

        @Override
        public Consumer<Object> visit(ShortType shortType) {
            final ShortAppender app = ShortAppender.get(appender);
            return arg -> ShortAppender.append(app, (short) arg);
        }

        @Override
        public Consumer<Object> visit(IntType intType) {
            final IntAppender app = IntAppender.get(appender);
            return arg -> IntAppender.append(app, (int) arg);
        }

        @Override
        public Consumer<Object> visit(LongType longType) {
            // todo: ability to ingest millis via long w/ annotation
            final LongAppender app = LongAppender.get(appender);
            return arg -> LongAppender.append(app, (long) arg);
        }

        @Override
        public Consumer<Object> visit(FloatType floatType) {
            final FloatAppender app = FloatAppender.get(appender);
            return arg -> FloatAppender.append(app, (float) arg);
        }

        @Override
        public Consumer<Object> visit(DoubleType doubleType) {
            final DoubleAppender app = DoubleAppender.get(appender);
            return arg -> DoubleAppender.append(app, (double) arg);
        }

        @Override
        public Consumer<Object> visit(BoxedBooleanType booleanType) {
            final BooleanAppender app = BooleanAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    BooleanAppender.appendNull(app);
                } else {
                    BooleanAppender.append(app, (boolean) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedByteType byteType) {
            final ByteAppender app = ByteAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    ByteAppender.appendNull(app);
                } else {
                    ByteAppender.append(app, (byte) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedCharType charType) {
            final CharAppender app = CharAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    CharAppender.appendNull(app);
                } else {
                    CharAppender.append(app, (char) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedShortType shortType) {
            final ShortAppender app = ShortAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    ShortAppender.appendNull(app);
                } else {
                    ShortAppender.append(app, (short) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedIntType intType) {
            final IntAppender app = IntAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    IntAppender.appendNull(app);
                } else {
                    IntAppender.append(app, (int) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedLongType longType) {
            final LongAppender app = LongAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    LongAppender.appendNull(app);
                } else {
                    LongAppender.append(app, (long) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedFloatType floatType) {
            final FloatAppender app = FloatAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    FloatAppender.appendNull(app);
                } else {
                    FloatAppender.append(app, (float) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(BoxedDoubleType doubleType) {
            final DoubleAppender app = DoubleAppender.get(appender);
            return arg -> {
                if (arg == null) {
                    DoubleAppender.appendNull(app);
                } else {
                    DoubleAppender.append(app, (double) arg);
                }
            };
        }

        @Override
        public Consumer<Object> visit(StringType stringType) {
            return generic(stringType);
        }

        @Override
        public Consumer<Object> visit(InstantType instantType) {
            return generic(instantType);
        }

        @Override
        public Consumer<Object> visit(ArrayType<?, ?> arrayType) {
            return generic(arrayType);
        }

        @Override
        public Consumer<Object> visit(CustomType<?> customType) {
            return generic(customType);
        }

        private <T> Consumer<Object> generic(GenericType<T> type) {
            final ObjectAppender<T> app = ObjectAppender.get(appender, type);
            final Class<T> clazz = type.clazz();
            return arg -> {
                if (arg == null) {
                    ObjectAppender.appendNull(app);
                } else {
                    ObjectAppender.append(app, clazz.cast(arg));
                }
            };
        }
    }

    private static class SetVisitor2<X> implements Type.Visitor<Consumer<X>>, PrimitiveType.Visitor<Consumer<X>>,
            GenericType.Visitor<Consumer<X>>, BoxedType.Visitor<Consumer<X>> {
        public static <X> Consumer<X> of(Appender appender, Method method) {
            return Objects.requireNonNull(appender.type().walk(new SetVisitor2<>(appender, method)));
        }

        private final Appender appender;
        private final Method method;
        // private final MethodGetter<T> getter;

        private SetVisitor2(Appender appender, Method method) {
            this.appender = Objects.requireNonNull(appender);
            this.method = Objects.requireNonNull(method);
        }

        @Override
        public Consumer<X> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Consumer<X>>) this);
        }

        @Override
        public Consumer<X> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Consumer<X>>) this);
        }

        @Override
        public Consumer<X> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Consumer<X>>) this);
        }

        @Override
        public Consumer<X> visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public Consumer<X> visit(ByteType byteType) {
            return null;
        }

        @Override
        public Consumer<X> visit(CharType charType) {
            return null;
        }

        @Override
        public Consumer<X> visit(ShortType shortType) {
            return null;
        }

        @Override
        public Consumer<X> visit(IntType intType) {
            final IntAppender app = IntAppender.get(appender);
            final ToIntFunction<X> getter = MethodGetter.ofInt(method);
            return x -> app.set(getter.applyAsInt(x));
        }

        @Override
        public Consumer<X> visit(LongType longType) {
            final LongAppender app = LongAppender.get(appender);
            final ToLongFunction<X> getter = MethodGetter.ofLong(method);
            return x -> app.set(getter.applyAsLong(x));
        }

        @Override
        public Consumer<X> visit(FloatType floatType) {
            return null;
        }

        @Override
        public Consumer<X> visit(DoubleType doubleType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedBooleanType booleanType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedByteType byteType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedCharType charType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedShortType shortType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedIntType intType) {
            final IntAppender app = IntAppender.get(appender);
            final ToObjectFunction<X, Integer> getter = MethodGetter.ofGeneric(method, intType);
            return x -> {
                final Integer out = getter.apply(x);
                if (out == null) {
                    app.setNull();
                } else {
                    app.set(out);
                }
            };
        }

        @Override
        public Consumer<X> visit(BoxedLongType longType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedFloatType floatType) {
            return null;
        }

        @Override
        public Consumer<X> visit(BoxedDoubleType doubleType) {
            return null;
        }

        @Override
        public Consumer<X> visit(StringType stringType) {
            return null;
        }

        @Override
        public Consumer<X> visit(InstantType instantType) {
            return null;
        }

        @Override
        public Consumer<X> visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public Consumer<X> visit(CustomType<?> customType) {
            return null;
        }
    }
}
