//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

final class ArrayUtilities {
    static char[] duplicate(int times, char[] x) {
        char[] res = new char[x.length * times];
        for (int i = 0; i < times; i++) {
            System.arraycopy(x, 0, res, x.length * i, x.length);
        }
        return res;
    }

    static float[] duplicate(int times, float[] x) {
        float[] res = new float[x.length * times];
        for (int i = 0; i < times; i++) {
            System.arraycopy(x, 0, res, x.length * i, x.length);
        }
        return res;
    }

    static double[] duplicate(int times, double[] x) {
        double[] res = new double[x.length * times];
        for (int i = 0; i < times; i++) {
            System.arraycopy(x, 0, res, x.length * i, x.length);
        }
        return res;
    }
}
