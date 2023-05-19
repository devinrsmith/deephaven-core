package io.deephaven.engine.table.impl.strings;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;

import java.util.stream.Collectors;

class Impl {
    public static String of(Function function) {
        // <name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return function.name()
                + function.arguments().stream().map(ExpressionString::of).collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(Method method) {
        // (<obj-exp>).<name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return "("
                + ExpressionString.of(method.object())
                + ")." + method.name()
                + method.arguments()
                        .stream()
                        .map(ExpressionString::of)
                        .collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }
}
