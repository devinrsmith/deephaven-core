package io.deephaven.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SortOrderSelectionTest {

    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
                    Types.NestedField.optional(3, "symbol", Types.StringType.get()),
                    Types.NestedField.optional(4, "price", Types.DoubleType.get()));

    /** The order the data is written in: [ts, symbol]. */
    private static final SortOrder DESIRED =
            SortOrder.builderFor(SCHEMA).asc("ts").asc("symbol").build();

    /**
     * Hand-built orders with explicit IDs, used where tests need full control over IDs or need
     * situations a real table cannot hold (such as two equivalent orders with different IDs).
     */
    private static final SortOrder TS = SortOrder.builderFor(SCHEMA).withOrderId(1).asc("ts").build();

    private static final SortOrder DAY_TS =
            SortOrder.builderFor(SCHEMA).withOrderId(2).asc(Expressions.day("ts")).build();

    private static final SortOrder DAY_TS_SYMBOL =
            SortOrder.builderFor(SCHEMA)
                    .withOrderId(3)
                    .asc(Expressions.day("ts"))
                    .asc("symbol")
                    .build();

    private static final SortOrder SYMBOL =
            SortOrder.builderFor(SCHEMA).withOrderId(4).asc("symbol").build();

    private static final SortOrder TS_DESC =
            SortOrder.builderFor(SCHEMA).withOrderId(5).desc("ts").build();

    /** Equivalent to {@link #TS} but under a different ID; impossible in a real table. */
    private static final SortOrder TS_DUPLICATE =
            SortOrder.builderFor(SCHEMA).withOrderId(6).asc("ts").build();

    private static final SortOrder TS_SYMBOL =
            SortOrder.builderFor(SCHEMA).withOrderId(7).asc("ts").asc("symbol").build();

    @Nested
    class BestDeclaredOrderFromCandidates {

        private final Comparator<SortOrder> tieBreaker = SortOrderSelection.defaultTieBreaker(-1);

        @Test
        void returnsUnsortedWhenThereAreNoCandidates() {
            SortOrder result = SortOrderSelection.bestDeclaredOrder(List.of(), DESIRED, tieBreaker);

            assertThat(result.isUnsorted()).isTrue();
        }

        @Test
        void returnsUnsortedWhenNoCandidateIsSatisfied() {
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(List.of(SYMBOL, TS_DESC), DESIRED, tieBreaker);

            assertThat(result.isUnsorted()).isTrue();
        }

        @Test
        void returnsUnsortedWhenOnlyUnsortedIsRegistered() {
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(
                            List.of(SortOrder.unsorted()), DESIRED, tieBreaker);

            assertThat(result.isUnsorted()).isTrue();
        }

        @Test
        void prefersExactMatch() {
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(
                            List.of(TS, DAY_TS, DAY_TS_SYMBOL, TS_SYMBOL), DESIRED, tieBreaker);

            assertThat(result).isEqualTo(TS_SYMBOL);
        }

        @Test
        void finerTransformDominatesCoarserOneRegardlessOfTieBreaker() {
            // Even when the tie-breaker favors DAY_TS as the default, TS strictly dominates it.
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(
                            List.of(DAY_TS, TS),
                            DESIRED,
                            SortOrderSelection.defaultTieBreaker(DAY_TS.orderId()));

            assertThat(result).isEqualTo(TS);
        }

        @Test
        void ignoresCandidatesThatAreNotSatisfied() {
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(
                            List.of(SYMBOL, TS_DESC, DAY_TS), DESIRED, tieBreaker);

            assertThat(result).isEqualTo(DAY_TS);
        }

        @Test
        void defaultTieBreakerPrefersMoreFieldsAmongIncomparableCandidates() {
            // [ts] and [day(ts), symbol] are both satisfied by [ts, symbol] but incomparable.
            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(List.of(TS, DAY_TS_SYMBOL), DESIRED, tieBreaker);

            assertThat(result).isEqualTo(DAY_TS_SYMBOL);
        }

        @Test
        void equivalentCandidatesAreResolvedByTieBreaker() {
            SortOrder preferLower =
                    SortOrderSelection.bestDeclaredOrder(
                            List.of(TS, TS_DUPLICATE), DESIRED, SortOrderSelection.defaultTieBreaker(TS.orderId()));
            SortOrder preferHigher =
                    SortOrderSelection.bestDeclaredOrder(List.of(TS, TS_DUPLICATE), DESIRED, tieBreaker);

            assertThat(preferLower).isEqualTo(TS);
            assertThat(preferHigher).isEqualTo(TS_DUPLICATE);
        }

        @Test
        void customTieBreakerDecidesAmongIncomparableCandidates() {
            Comparator<SortOrder> fewerFields =
                    Comparator.comparingInt((SortOrder o) -> o.fields().size()).reversed();

            SortOrder result =
                    SortOrderSelection.bestDeclaredOrder(List.of(TS, DAY_TS_SYMBOL), DESIRED, fewerFields);

            assertThat(result).isEqualTo(TS);
        }

        @Test
        void customTieBreakerCannotPromoteADominatedCandidate() {
            // Unsorted is dominated by every satisfied sorted order, so a tie-breaker that
            // prefers fewer fields still cannot select it.
            Comparator<SortOrder> fewerFields =
                    Comparator.comparingInt((SortOrder o) -> o.fields().size()).reversed();

            SortOrder result = SortOrderSelection.bestDeclaredOrder(List.of(TS), DESIRED, fewerFields);

            assertThat(result).isEqualTo(TS);
        }

        @Test
        void rejectsNullArguments() {
            assertThatThrownBy(() -> SortOrderSelection.bestDeclaredOrder(null, DESIRED, tieBreaker))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> SortOrderSelection.bestDeclaredOrder(List.of(TS), null, tieBreaker))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> SortOrderSelection.bestDeclaredOrder(List.of(TS), DESIRED, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    /**
     * Tests against real tables in an {@link InMemoryCatalog}. Order IDs are assigned by table
     * metadata, so each test captures orders as registered rather than using the constants above.
     */
    @Nested
    class BestDeclaredOrderFromTable {

        private InMemoryCatalog catalog;

        @BeforeEach
        void setUp() {
            catalog = new InMemoryCatalog();
            catalog.initialize("test", Map.of());
            catalog.createNamespace(Namespace.of("db"));
        }

        @AfterEach
        void tearDown() throws IOException {
            catalog.close();
        }

        private Table createTable(Consumer<SortOrder.Builder> initialOrder) {
            SortOrder.Builder builder = SortOrder.builderFor(SCHEMA);
            initialOrder.accept(builder);
            return catalog
                    .buildTable(TableIdentifier.of("db", "t"), SCHEMA)
                    .withSortOrder(builder.build())
                    .create();
        }

        /** Commits a new sort order, which also becomes the default, and returns it as registered. */
        private SortOrder register(Table table, Consumer<ReplaceSortOrder> fields) {
            ReplaceSortOrder replace = table.replaceSortOrder();
            fields.accept(replace);
            replace.commit();
            table.refresh();
            return table.sortOrder();
        }

        @Test
        void equivalentOrderReusesExistingId() {
            // Documents the assumption behind this class's tests: a real table never holds two
            // equivalent orders, because re-registering one reuses its ID.
            Table table = createTable(b -> b.asc("ts"));
            SortOrder first = table.sortOrder();
            register(table, r -> r.asc("symbol"));

            SortOrder again = register(table, r -> r.asc("ts"));

            assertThat(again.orderId()).isEqualTo(first.orderId());
        }

        @Test
        void selectsMostSpecificRegisteredOrder() {
            Table table = createTable(b -> b.asc(Expressions.day("ts")));
            SortOrder ts = register(table, r -> r.asc("ts"));
            register(table, r -> r.asc("symbol")); // unsatisfied, and now the default

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED)).isEqualTo(ts);
        }

        @Test
        void prefersExactMatchOverPrefixes() {
            Table table = createTable(b -> b.asc("ts"));
            SortOrder tsSymbol = register(table, r -> r.asc("ts").asc("symbol"));
            register(table, r -> r.asc(Expressions.day("ts")));

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED)).isEqualTo(tsSymbol);
        }

        @Test
        void finerTransformWinsEvenWhenCoarserIsDefault() {
            Table table = createTable(b -> b.asc("ts"));
            SortOrder ts = table.sortOrder();
            register(table, r -> r.asc(Expressions.day("ts"))); // now the default

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED)).isEqualTo(ts);
        }

        @Test
        void prefersDefaultAmongIncomparableOrdersOfEqualLength() {
            // [day(ts), symbol] and [ts, truncate(symbol, 2)] are both satisfied by [ts, symbol],
            // have the same length, and neither satisfies the other.
            Table table = createTable(b -> b.asc(Expressions.day("ts")).asc("symbol"));
            SortOrder daySymbol = table.sortOrder();
            SortOrder tsTruncated =
                    register(table, r -> r.asc("ts").asc(Expressions.truncate("symbol", 2)));

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED)).isEqualTo(tsTruncated);

            // Switching the default back reuses the existing ID and flips the choice.
            register(table, r -> r.asc(Expressions.day("ts")).asc("symbol"));

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED)).isEqualTo(daySymbol);
        }

        @Test
        void returnsUnsortedWhenNoRegisteredOrderIsSatisfied() {
            Table table = createTable(b -> b.asc("symbol"));
            register(table, r -> r.desc("ts"));

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED).isUnsorted()).isTrue();
        }

        @Test
        void returnsUnsortedForUnsortedTable() {
            Table table =
                    catalog.buildTable(TableIdentifier.of("db", "unsorted"), SCHEMA).create();

            assertThat(SortOrderSelection.bestDeclaredOrder(table, DESIRED).isUnsorted()).isTrue();
        }

        @Test
        void rejectsNullTable() {
            assertThatThrownBy(() -> SortOrderSelection.bestDeclaredOrder((Table) null, DESIRED))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class StrictlyMoreSpecific {

        @Test
        void identityIsStrictlyMoreSpecificThanCoarserTransform() {
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, DAY_TS)).isTrue();
            assertThat(SortOrderSelection.strictlyMoreSpecific(DAY_TS, TS)).isFalse();
        }

        @Test
        void longerPrefixIsStrictlyMoreSpecific() {
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS_SYMBOL, TS)).isTrue();
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, TS_SYMBOL)).isFalse();
        }

        @Test
        void anySortedOrderIsStrictlyMoreSpecificThanUnsorted() {
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, SortOrder.unsorted())).isTrue();
            assertThat(SortOrderSelection.strictlyMoreSpecific(SortOrder.unsorted(), TS)).isFalse();
        }

        @Test
        void equivalentOrdersAreNotStrictlyMoreSpecific() {
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, TS_DUPLICATE)).isFalse();
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS_DUPLICATE, TS)).isFalse();
        }

        @Test
        void incomparableOrdersAreNotStrictlyMoreSpecific() {
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, DAY_TS_SYMBOL)).isFalse();
            assertThat(SortOrderSelection.strictlyMoreSpecific(DAY_TS_SYMBOL, TS)).isFalse();
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS, TS_DESC)).isFalse();
            assertThat(SortOrderSelection.strictlyMoreSpecific(TS_DESC, TS)).isFalse();
        }
    }

    @Nested
    class DefaultTieBreaker {

        @Test
        void prefersMoreFields() {
            Comparator<SortOrder> cmp = SortOrderSelection.defaultTieBreaker(TS.orderId());

            // More fields wins even over the default order.
            assertThat(cmp.compare(DAY_TS_SYMBOL, TS)).isPositive();
        }

        @Test
        void prefersDefaultOrderWhenFieldCountsMatch() {
            Comparator<SortOrder> cmp = SortOrderSelection.defaultTieBreaker(TS.orderId());

            assertThat(cmp.compare(TS, TS_DUPLICATE)).isPositive();
        }

        @Test
        void prefersHigherOrderIdWhenNeitherIsDefault() {
            Comparator<SortOrder> cmp = SortOrderSelection.defaultTieBreaker(-1);

            assertThat(cmp.compare(TS_DUPLICATE, TS)).isPositive();
        }

        @Test
        void isConsistentForTheSameOrder() {
            Comparator<SortOrder> cmp = SortOrderSelection.defaultTieBreaker(TS.orderId());

            assertThat(cmp.compare(TS, TS)).isZero();
        }
    }
}