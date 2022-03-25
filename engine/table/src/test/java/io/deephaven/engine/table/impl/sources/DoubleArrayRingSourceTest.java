package io.deephaven.engine.table.impl.sources;

import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public class DoubleArrayRingSourceTest {

    @Test
    public void sizes() {
        for (int n = 1; n < 100; ++n) {
            final DoubleArrayRingSource ring = new DoubleArrayRingSource(n);
            checkSize(ring, 0);
            assertThat(ring.n()).isEqualTo(n);
            for (int i = 0; i < n; ++i) {
                ring.add(i);
                checkSize(ring, i + 1);
            }
            for (int i = 0; i < n + 10; ++i) {
                ring.add(n + i);
                checkSize(ring, n);
            }
        }
    }

    @Test
    public void containsIndex() {
        final DoubleArrayRingSource ring = new DoubleArrayRingSource(2);

        ring.add(0.0);
        checkContainsIndex(ring, 0, 0);

        ring.add(1.0);
        checkContainsIndex(ring, 0, 1);

        ring.add(2.0);
        checkContainsIndex(ring, 1, 2);

        ring.add(3.0);
        checkContainsIndex(ring, 2, 3);

        ring.add(4.0);
        checkContainsIndex(ring, 3, 4);

        ring.add(new double[] { 5.0, 6.0 });
        checkContainsIndex(ring, 5, 6);

        ring.add(new double[] { 7.0 });
        checkContainsIndex(ring, 6, 7);

        ring.add(new double[] { 8.0, 9.0, 10.0 });
        checkContainsIndex(ring, 9, 10);

        ring.add(new double[] { 11.0, 12.0, 13.0, 14.0 });
        checkContainsIndex(ring, 13, 14);

        ring.add(new double[] { 15.0, 16.0, 17.0, 18.0, 19.0 });
        checkContainsIndex(ring, 18, 19);
    }

    private static void checkContainsIndex(DoubleArrayRingSource ring, long startIx, long endIx) {
        checkSize(ring, (int)(endIx - startIx + 1));
        for (long i = startIx; i <= endIx; ++i) {
            assertThat(ring.containsIndex(i)).isTrue();
            assertThat(ring.getDouble(i)).isEqualTo((double)i);
        }
        for (int i = 1; i < 100; ++i) {
            assertThat(ring.containsIndex(startIx - i)).isFalse();
            assertThat(ring.getDouble(startIx - i)).isEqualTo(NULL_DOUBLE);

            assertThat(ring.containsIndex(endIx + i)).isFalse();
            assertThat(ring.getDouble(endIx + i)).isEqualTo(NULL_DOUBLE);
        }
    }

    private static void checkSize(DoubleArrayRingSource ring, int expectedSize) {
        assertThat(ring.isEmpty()).isEqualTo(expectedSize == 0);
        assertThat(ring.size()).isEqualTo(expectedSize);
        assertThat(ring.indices().ixCardinality()).isEqualTo(expectedSize);
    }
}
