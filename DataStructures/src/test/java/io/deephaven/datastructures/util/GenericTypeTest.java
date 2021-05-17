package io.deephaven.datastructures.util;

import org.junit.Assert;
import org.junit.Test;

public class GenericTypeTest {

  @Test
  public void noGenericInt() {
      try {
          GenericType.of(int.class);
          Assert.fail("Expected exception");
      } catch (IllegalArgumentException e) {
          // expected
      }
  }

    @Test
    public void noGenericInteger() {
        try {
            GenericType.of(Integer.class);
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDoublePrimitive() {
        try {
            GenericType.of(double.class);
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDouble() {
        try {
            GenericType.of(Double.class);
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericString() {
        try {
            GenericType.of(String.class);
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
