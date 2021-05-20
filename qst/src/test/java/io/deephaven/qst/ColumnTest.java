package io.deephaven.qst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class ColumnTest {

    @Test
    public void noIntegerMinValue() {
        try {
            Column.of("AnInt", 1, Integer.MIN_VALUE, 3);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void intHelper() {
        Column<Integer> expected = ImmutableColumn.<Integer>builder()
            .header(ImmutableColumnHeader.<Integer>builder()
                .name("AnInt")
                .type(IntType.instance())
                .build())
            .addValues(1, null, 3)
            .build();

        Column<Integer> actual = Column.of("AnInt", 1, null, 3);

        assertThat(actual).isEqualTo(expected);
    }


    @Test
    public void noDoubleNegativeMax() {
        try {
            Column.of("ADouble", 1.0, -Double.MAX_VALUE, 3.0);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void doubleHelper() {
        Column<Double> expected = ImmutableColumn.<Double>builder()
            .header(ImmutableColumnHeader.<Double>builder()
                .name("ADouble")
                .type(DoubleType.instance())
                .build())
            .addValues(1., null, 3.)
            .build();

        Column<Double> actual = Column.of("ADouble", 1., null, 3.);

        assertThat(actual).isEqualTo(expected);
    }
}
