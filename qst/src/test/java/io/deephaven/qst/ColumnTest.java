package io.deephaven.qst;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ColumnTest {

    @Test
    public void intHelper() {
        Column<Integer> expected = ImmutableColumn.<Integer>builder()
            .header(ColumnHeader.ofInt("AnInt"))
            .addValues(1, null, 3)
            .build();

        Column<Integer> actual = Column.of("AnInt", 1, null, 3);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void doubleHelper() {
        Column<Double> expected = ImmutableColumn.<Double>builder()
            .header(ColumnHeader.ofDouble("ADouble"))
            .addValues(1., null, 3.)
            .build();

        Column<Double> actual = Column.of("ADouble", 1., null, 3.);

        assertThat(actual).isEqualTo(expected);
    }
}
