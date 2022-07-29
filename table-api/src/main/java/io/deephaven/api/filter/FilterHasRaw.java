/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.RawString;

public enum FilterHasRaw implements Filter.Visitor<Boolean> {
    INSTANCE;

    public static boolean of(Filter filter) {
        return filter.walk(INSTANCE);
    }

    @Override
    public Boolean visit(FilterIsNull isNull) {
        return false;
    }

    @Override
    public Boolean visit(FilterIsNotNull isNotNull) {
        return false;
    }

    @Override
    public Boolean visit(FilterCondition condition) {
        return false;
    }

    @Override
    public Boolean visit(FilterNot not) {
        return not.filter().walk(this);
    }

    @Override
    public Boolean visit(FilterOr ors) {
        return ors.filters().stream().anyMatch(FilterHasRaw::of);
    }

    @Override
    public Boolean visit(FilterAnd ands) {
        return ands.filters().stream().anyMatch(FilterHasRaw::of);
    }

    @Override
    public Boolean visit(RawString rawString) {
        return true;
    }
}
