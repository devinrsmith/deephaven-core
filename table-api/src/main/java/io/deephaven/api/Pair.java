/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A column pair represents an {@link #input() input} and an {@link #output() output} column.
 */
public interface Pair {

    static Pair of(ColumnName input, ColumnName output) {
        if (input.equals(output)) {
            return input;
        }
        return PairImpl.of(input, output);
    }

    static Pair parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0) {
            return ColumnName.parse(x);
        }
        ColumnName output = ColumnName.parse(x.substring(0, ix));
        ColumnName input = ColumnName.parse(x.substring(ix + 1));
        return of(input, output);
    }

    static List<Pair> from(String... values) {
        return from(Arrays.asList(values));
    }

    static List<Pair> from(Collection<String> values) {
        return values.stream().map(Pair::parse).collect(Collectors.toList());
    }

    /**
     * Produces an RPC-string, compatible with {@link #parse(String)}.
     *
     * @return the RPC-string
     */
    static String toRpcString(Pair pair) {
        if (pair.input().equals(pair.output())) {
            return pair.output().toRpcString();
        }
        return pair.output().toRpcString() + "=" + pair.input().toRpcString();
    }

    /**
     * The input column.
     *
     * @return the input column
     */
    ColumnName input();

    /**
     * The output column.
     *
     * @return the output column
     */
    ColumnName output();
}
