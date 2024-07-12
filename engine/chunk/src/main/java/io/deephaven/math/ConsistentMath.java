//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Comparator;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public interface ConsistentMath {

    static ConsistentMath java() {
        return ConsistentMathJava.JAVA;
    }

    static ConsistentMath deephaven() {
        return ConsistentMathDh.INSTANCE;
    }

    static ConsistentMath bitwise() {
        return ConsistentMathBitwise.INSTANCE;
    }

    // --------------------------------

    boolean equals(float x, float y);

    boolean equals(double x, double y);

    // --------------------------------

    int hashCode(float x);

    int hashCode(double x);

    // --------------------------------


    int compare(float x, float y);

    int compare(double x, double y);

    // --------------------------------

    /**
     * The equivalent to {@link java.util.Arrays#equals(float[], float[])} with respect to
     * {@link #equals(float, float)}.
     */
    boolean equals(float[] x, float[] y);

    boolean equals(double[] x, double[] y);

    boolean equals(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo);

    boolean equals(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo);

    // --------------------------------

    int hashCode(float[] x);

    int hashCode(double[] x);

    int hashCode(float[] x, int xFrom, int xTo);

    int hashCode(double[] x, int xFrom, int xTo);

    // --------------------------------

    int compare(float[] x, float[] y);

    int compare(double[] x, double[] y);

    int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo);

    int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo);

    // --------------------------------

    int mismatch(float[] x, float[] y);

    int mismatch(double[] x, double[] y);

    int mismatch(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo);

    int mismatch(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo);

    // --------------------------------

    <T> BiPredicate<T, T> equals(Class<T> clazz);

    <T> BiPredicate<T, T> deepEquals(Class<T> clazz);

    // --------------------------------

    <T> ToIntFunction<T> hashCode(Class<T> clazz);

    <T> ToIntFunction<T> deepHashCode(Class<T> clazz);

    // --------------------------------

    <T> Comparator<T> comparator(Class<T> clazz);

    // --------------------------------
}
