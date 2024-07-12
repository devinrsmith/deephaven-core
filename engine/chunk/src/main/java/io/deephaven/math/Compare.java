//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Comparator;

public interface Compare {

    /**
     * -0.0f < 0.0f -0.0 < 0.0 DH nulls first, NaNs last
     */
    static Compare deephaven() {
        return CompareDeephaven.INSTANCE;
    }

    /**
     * -0.0f < 0.0f -0.0 < 0.0 NaNs last
     */
    static Compare java() {
        return CompareJava.INSTANCE;
    }

    /**
     * Not a "natural" order
     */
    static Compare bitwise() {
        return CompareBitwise.INSTANCE;
    }

    // -------------------------------

    Math math();

    // -------------------------------

    int compare(char x, char y);

    int compare(float x, float y);

    int compare(double x, double y);

    // --------------------------------

    int compare(char[] x, char[] y);

    int compare(float[] x, float[] y);

    int compare(double[] x, double[] y);

    int compare(char[] x, int xFrom, int xTo, char[] y, int yFrom, int yTo);

    int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo);

    int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo);

    // --------------------------------

    char min(char x, char y);

    float min(float x, float y);

    double min(double x, double y);

    // --------------------------------

    char max(char x, char y);

    float max(float x, float y);

    double max(double x, double y);

    // --------------------------------

    <T> Comparator<T> comparator(Class<T> clazz);
}
