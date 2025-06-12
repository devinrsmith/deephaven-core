//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.api.util.NameValidator;
import io.deephaven.json.AnyValue;
import io.deephaven.json.ArrayValue;
import io.deephaven.json.BigDecimalValue;
import io.deephaven.json.BigIntegerValue;
import io.deephaven.json.BoolValue;
import io.deephaven.json.ByteValue;
import io.deephaven.json.CharValue;
import io.deephaven.json.DoubleValue;
import io.deephaven.json.FloatValue;
import io.deephaven.json.InstantNumberValue;
import io.deephaven.json.InstantValue;
import io.deephaven.json.IntValue;
import io.deephaven.json.JsonValueTypes;
import io.deephaven.json.LocalDateValue;
import io.deephaven.json.LongValue;
import io.deephaven.json.ObjectEntriesValue;
import io.deephaven.json.ObjectField;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.ShortValue;
import io.deephaven.json.SkipValue;
import io.deephaven.json.StringValue;
import io.deephaven.json.TupleValue;
import io.deephaven.json.TypedObjectValue;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class Mixin<T extends Value> {

    static final Function<List<String>, String> TO_COLUMN_NAME = Mixin::toColumnName;

    public static String toColumnName(List<String> path) {
        return path.isEmpty() ? "Value" : String.join("_", path);
    }

    static Mixin<?> of(Value options) {
        return options.walk(MixinImpl.MIXIN_VISITOR);
    }

    final T options;

    Mixin(T options) {
        this.options = Objects.requireNonNull(options);
    }

    public JacksonIteratorProvider arrayProvider() {
        return new JIPArray();
    }

    public JacksonIteratorProvider streamProvider() {
        return new JIPStream();
    }

    public abstract int outputSize();

    public final List<Type<?>> outputTypes() {
        return outputTypesImpl().collect(Collectors.toList());
    }

    public final List<String> names() {
        return names(TO_COLUMN_NAME);
    }

    public final List<String> names(Function<List<String>, String> f) {
        return Arrays.asList(NameValidator.legalizeColumnNames(paths().map(f).toArray(String[]::new), true));
    }

    abstract ValueProcessor processor(String context);

    abstract RepeaterProcessor repeaterProcessor();

    abstract Stream<List<String>> paths();

    abstract Stream<Type<?>> outputTypesImpl();

    static List<String> prefixWith(String prefix, List<String> path) {
        return Stream.concat(Stream.of(prefix), path.stream()).collect(Collectors.toList());
    }

    static Stream<List<String>> prefixWithKeys(Map<ObjectField, Mixin<?>> fields) {
        final List<Stream<List<String>>> paths = new ArrayList<>(fields.size());
        for (Entry<ObjectField, Mixin<?>> e : fields.entrySet()) {
            final Stream<List<String>> prefixedPaths = e.getValue().paths().map(x -> prefixWith(e.getKey().name(), x));
            paths.add(prefixedPaths);
        }
        return paths.stream().flatMap(Function.identity());
    }

    static Stream<List<String>> prefixWithKeysAndSkip(Map<String, ? extends Mixin<?>> fields, int skip) {
        final List<Stream<List<String>>> paths = new ArrayList<>(fields.size());
        for (Entry<String, ? extends Mixin<?>> e : fields.entrySet()) {
            final Stream<List<String>> prefixedPaths =
                    e.getValue().paths().map(x -> prefixWith(e.getKey(), x)).skip(skip);
            paths.add(prefixedPaths);
        }
        return paths.stream().flatMap(Function.identity());
    }

    private enum MixinImpl implements Value.Visitor<Mixin<?>> {
        MIXIN_VISITOR;

        @Override
        public StringMixin visit(StringValue _string) {
            return new StringMixin(_string);
        }

        @Override
        public Mixin<?> visit(BoolValue _bool) {
            return new BoolMixin(_bool);
        }

        @Override
        public Mixin<?> visit(ByteValue _byte) {
            return new ByteMixin(_byte);
        }

        @Override
        public Mixin<?> visit(CharValue _char) {
            return new CharMixin(_char);
        }

        @Override
        public Mixin<?> visit(ShortValue _short) {
            return new ShortMixin(_short);
        }

        @Override
        public IntMixin visit(IntValue _int) {
            return new IntMixin(_int);
        }

        @Override
        public LongMixin visit(LongValue _long) {
            return new LongMixin(_long);
        }

        @Override
        public FloatMixin visit(FloatValue _float) {
            return new FloatMixin(_float);
        }

        @Override
        public DoubleMixin visit(DoubleValue _double) {
            return new DoubleMixin(_double);
        }

        @Override
        public ObjectMixin visit(ObjectValue object) {
            return new ObjectMixin(object);
        }

        @Override
        public Mixin<?> visit(ObjectEntriesValue objectKv) {
            return new ObjectEntriesMixin(objectKv);
        }

        @Override
        public InstantMixin visit(InstantValue instant) {
            return new InstantMixin(instant);
        }

        @Override
        public InstantNumberMixin visit(InstantNumberValue instantNumber) {
            return new InstantNumberMixin(instantNumber);
        }

        @Override
        public BigIntegerMixin visit(BigIntegerValue bigInteger) {
            return new BigIntegerMixin(bigInteger);
        }

        @Override
        public BigDecimalMixin visit(BigDecimalValue bigDecimal) {
            return new BigDecimalMixin(bigDecimal);
        }

        @Override
        public SkipMixin visit(SkipValue skip) {
            return new SkipMixin(skip);
        }

        @Override
        public TupleMixin visit(TupleValue tuple) {
            return new TupleMixin(tuple);
        }

        @Override
        public TypedObjectMixin visit(TypedObjectValue typedObject) {
            return new TypedObjectMixin(typedObject);
        }

        @Override
        public LocalDateMixin visit(LocalDateValue localDate) {
            return new LocalDateMixin(localDate);
        }

        @Override
        public ArrayMixin visit(ArrayValue array) {
            return new ArrayMixin(array);
        }

        @Override
        public AnyMixin visit(AnyValue any) {
            return new AnyMixin(any);
        }
    }

    final boolean allowNull() {
        return options.allowedTypes().contains(JsonValueTypes.NULL);
    }

    final boolean allowMissing() {
        return options.allowMissing();
    }

    final boolean allowNumberInt() {
        return options.allowedTypes().contains(JsonValueTypes.INT);
    }

    final boolean allowDecimal() {
        return options.allowedTypes().contains(JsonValueTypes.DECIMAL);
    }

    final void checkNumberAllowed(JsonParser parser) throws IOException {
        if (!allowNumberInt() && !allowDecimal()) {
            throw new ValueAwareException("Number not allowed", parser.currentLocation(), options);
        }
    }

    final void checkNumberIntAllowed(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw new ValueAwareException("Number int not allowed", parser.currentLocation(), options);
        }
    }

    final void checkDecimalAllowed(JsonParser parser) throws IOException {
        if (!allowDecimal()) {
            throw new ValueAwareException("Decimal not allowed", parser.currentLocation(), options);
        }
    }

    final void checkBoolAllowed(JsonParser parser) throws IOException {
        if (!options.allowedTypes().contains(JsonValueTypes.BOOL)) {
            throw new ValueAwareException("Bool not allowed", parser.currentLocation(), options);
        }
    }

    final void checkStringAllowed(JsonParser parser) throws IOException {
        if (!options.allowedTypes().contains(JsonValueTypes.STRING)) {
            throw new ValueAwareException("String not allowed", parser.currentLocation(), options);
        }
    }

    final void checkObjectAllowed(JsonParser parser) throws IOException {
        if (!options.allowedTypes().contains(JsonValueTypes.OBJECT)) {
            throw new ValueAwareException("Object not allowed", parser.currentLocation(), options);
        }
    }

    final void checkArrayAllowed(JsonParser parser) throws IOException {
        if (!options.allowedTypes().contains(JsonValueTypes.ARRAY)) {
            throw new ValueAwareException("Array not allowed", parser.currentLocation(), options);
        }
    }

    final void checkNullAllowed(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw nullNotAllowed(parser);
        }
    }

    final ValueAwareException nullNotAllowed(JsonParser parser) {
        return new ValueAwareException("Null not allowed", parser.currentLocation(), options);
    }

    final void checkMissingAllowed(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw new ValueAwareException("Missing not allowed", parser.currentLocation(), options);
        }
    }

    final IOException unexpectedToken(JsonParser parser) throws ValueAwareException {
        final String msg;
        switch (parser.currentToken()) {
            case VALUE_TRUE:
            case VALUE_FALSE:
                msg = "Bool not expected";
                break;
            case START_OBJECT:
                msg = "Object not expected";
                break;
            case START_ARRAY:
                msg = "Array not expected";
                break;
            case VALUE_NUMBER_INT:
                msg = "Number int not expected";
                break;
            case VALUE_NUMBER_FLOAT:
                msg = "Decimal not expected";
                break;
            case FIELD_NAME:
                msg = "Field name not expected";
                break;
            case VALUE_STRING:
                msg = "String not expected";
                break;
            case VALUE_NULL:
                msg = "Null not expected";
                break;
            default:
                msg = parser.currentToken() + " not expected";
        }
        throw new ValueAwareException(msg, parser.currentLocation(), options);
    }

    abstract class ValueProcessorMixinBase implements ValueProcessor {
        @Override
        public final int numColumns() {
            return Mixin.this.outputSize();
        }

        @Override
        public final Stream<Type<?>> columnTypes() {
            return Mixin.this.outputTypesImpl();
        }

        @Override
        public final void processCurrentValue(JsonParser parser) throws IOException {
            try {
                processCurrentValueImpl(parser);
            } catch (ValueAwareException e) {
                if (options.equals(e.value())) {
                    throw e;
                } else {
                    throw wrap(parser, e, "Unable to process current value");
                }
            } catch (IOException e) {
                throw wrap(parser, e, "Unable to process current value");
            }
        }

        @Override
        public final void processMissing(JsonParser parser) throws IOException {
            try {
                processMissingImpl(parser);
            } catch (ValueAwareException e) {
                if (options.equals(e.value())) {
                    throw e;
                } else {
                    throw wrap(parser, e, "Unable to process missing value");
                }
            } catch (IOException e) {
                throw wrap(parser, e, "Unable to process missing value");
            }
        }

        protected abstract void processCurrentValueImpl(JsonParser parser) throws IOException;

        protected abstract void processMissingImpl(JsonParser parser) throws IOException;

        private ValueAwareException wrap(JsonParser parser, IOException e, String msg) {
            return new ValueAwareException(msg, parser.currentLocation(), e, options);
        }
    }


    final class JIPArray extends JIPBase {

        @Override
        public JacksonIterator iterator(JsonParser parser, int bufferSize) throws IOException {
            return new JacksonArrayIterator(processor("<root>"), parser, bufferSize);
        }
    }

    final class JIPStream extends JIPBase {

        @Override
        public JacksonIterator iterator(JsonParser parser, int bufferSize) throws IOException {
            return new JacksonStreamIterator(processor("<root>"), parser, bufferSize);
        }
    }

    abstract class JIPBase implements JacksonIteratorProvider {

        @Override
        public final Value options() {
            return Mixin.this.options;
        }

        @Override
        public final List<Type<?>> outputTypes() {
            return Mixin.this.outputTypes();
        }

        @Override
        public final int outputSize() {
            return Mixin.this.outputSize();
        }

        @Override
        public final List<String> names() {
            return Mixin.this.names();
        }

        @Override
        public final List<String> names(Function<List<String>, String> f) {
            return Mixin.this.names(f);
        }
    }
}
