package io.deephaven.march;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BracketOrdering {

    private static int t(int L, int k) {
        if (k == 1) {
            return 1;
        }
        // a(n) = a & (-a)
        // https://oeis.org/A006519
        int k1 = k - 1;
        int m = k1 & (-k1);
        return 1 + L / m - t(L, k - m);
    }

    /**
     * Generates the bracket-optimal 1-based seed ordering for a bracket with {@code n} rounds.
     *
     * <p>
     * Optimal means that none of the top {@code 2^p} ranked players can meet earlier than the {@code p-th} from last
     * round of the competition.
     *
     * @param n the number of rounds
     * @return the 1-based seed ordering, with {@code 2^n} elements
     * @see <a href="https://oeis.org/A208569">A208569</a>
     */
    public static IntStream bracketOptimalSeedOrder(int n) {
        final int L = 1 << n;
        return IntStream.rangeClosed(1, L).map(i -> t(L, i));
    }

    /**
     * Generates the bracket-optimal ordering for the seed-ordered {@code items}.
     *
     * <p>
     * Optimal means that none of the top {@code 2^p} ranked players can meet earlier than the {@code p-th} from last
     * round of the competition.
     *
     * @param items the items in seed-order, must have a power-of-2 size
     * @param <T> the type
     * @return the items in bracket-optimal order
     * @see <a href="https://oeis.org/A208569">A208569</a>
     */
    public static <T> Stream<T> bracketOptimalOrder(List<T> items) {
        final int L = items.size();
        if ((L & (L - 1)) != 0) {
            throw new IllegalArgumentException("items must have a power-of-2 size");
        }
        return IntStream.rangeClosed(1, L).map(i -> t(L, i) - 1).mapToObj(items::get);
    }
}
