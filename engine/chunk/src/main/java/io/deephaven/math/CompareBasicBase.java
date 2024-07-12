//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Arrays;

abstract class CompareBasicBase extends CompareBase {

    CompareBasicBase(Equals math) {
        super(math);
    }

    @Override
    public final int compare(char x, char y) {
        return Character.compare(x, y);
    }

    @Override
    public final int compare(char[] x, char[] y) {
        return Arrays.compare(x, y);
    }

    @Override
    public final int compare(char[] x, int xFrom, int xTo, char[] y, int yFrom, int yTo) {
        return Arrays.compare(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public final char min(char x, char y) {
        return (char) java.lang.Math.min(x, y);
    }

    @Override
    public final char max(char x, char y) {
        return (char) java.lang.Math.max(x, y);
    }
}
