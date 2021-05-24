package io.deephaven.db.tables.utils;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.StreamSupport;

class CsvInspector {

    interface Parser<T> {
        T parse(String value) throws IllegalArgumentException;
    }

    enum BooleanParser implements Parser<Boolean> {
        INSTANCE;

        @Override
        public Boolean parse(String value) throws IllegalArgumentException {
            if (value.isEmpty()) {
                return null;
            }
            return Boolean.parseBoolean(value);
        }
    }

    enum LongParser implements Parser<Long> {
        INSTANCE;

        @Override
        public Long parse(String value) throws IllegalArgumentException {
            if (value.isEmpty()) {
                return null;
            }
            return Long.parseLong(value);
        }
    }

    enum DoubleParser implements Parser<Double> {
        INSTANCE;

        @Override
        public Double parse(String value) throws IllegalArgumentException {
            if (value.isEmpty()) {
                return null;
            }
            return Double.parseDouble(value);
        }
    }

    enum StringParser implements Parser<String> {
        INSTANCE;

        @Override
        public String parse(String value) throws IllegalArgumentException {
            if (value.isEmpty()) {
                return null; // can't represent empty w/ this scheme, todo
            }
            return value;
        }
    }

    public static <T> boolean matches(Parser<T> parser, String item) {
        try {
            parser.parse(item);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static <T> boolean allMatch(Parser<T> parser, Iterable<String> items) {
        return StreamSupport
            .stream(items.spliterator(), false)
            .allMatch(i -> matches(parser, i));
    }

    public static Optional<Parser<?>> firstMatchingParser(Iterable<Parser<?>> parsers, Iterable<String> items) {
        for (Parser<?> parser : parsers) {
            if (allMatch(parser, items)) {
                return Optional.of(parser);
            }
        }
        return Optional.empty();
    }

    public static Parser<?> inferCsvParser(Iterable<String> items) {
        return firstMatchingParser(Arrays.asList(
            BooleanParser.INSTANCE,
            LongParser.INSTANCE,
            DoubleParser.INSTANCE,
            StringParser.INSTANCE), items)
            .orElseThrow(IllegalStateException::new);
    }
}
