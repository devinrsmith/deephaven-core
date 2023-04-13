/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RawString;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.expression.AsOfJoinMatchFactory;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.qst.TableCreationLogic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.deephaven.api.TableOperationsDefaults.splitToCollection;

public abstract class TableBase implements TableSpec, TableOperationsDefaults<TableSpec, TableSpec> {

    // Note: method implementations should use the static constructors, or builder patterns, for the
    // necessary TableSpec instead of delegating to other methods. The default values are the
    // responsibility of the TableSpec.

    @Override
    public final TableCreationLogic logic() {
        return new TableCreationLogicImpl(this);
    }

    @Override
    public final HeadTable head(long size) {
        return HeadTable.of(this, size);
    }

    @Override
    public final TailTable tail(long size) {
        return TailTable.of(this, size);
    }

    @Override
    public final ReverseTable reverse() {
        return ReverseTable.of(this);
    }

    @Override
    public final SnapshotTable snapshot() {
        return SnapshotTable.of(this);
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, Flag... features) {
        return SnapshotWhenTable.of(this, trigger, SnapshotWhenOptions.of(features));
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, Collection<Flag> features,
            String... stampColumns) {
        return SnapshotWhenTable.of(this, trigger, SnapshotWhenOptions.of(features, stampColumns));
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, SnapshotWhenOptions options) {
        return SnapshotWhenTable.of(this, trigger, options);
    }

    @Override
    public final SortTable sort(Collection<SortColumn> columnsToSortBy) {
        return SortTable.builder().parent(this).addAllColumns(columnsToSortBy).build();
    }

    @Override
    public final WhereTable where(Collection<? extends Filter> filters) {
        return WhereTable.builder().parent(this).addAllFilters(filters).build();
    }

    @Override
    public final WhereInTable whereIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereIn(rightTable, columnsToMatch, false);
    }

    @Override
    public final WhereInTable whereNotIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereIn(rightTable, columnsToMatch, true);
    }

    private WhereInTable whereIn(TableSpec rightTable, String[] columnsToMatch, boolean inverted) {
        WhereInTable.Builder builder = WhereInTable.builder()
                .left(this)
                .right(rightTable)
                .inverted(inverted);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    private WhereInTable whereIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch,
            boolean inverted) {
        return WhereInTable.builder()
                .left(this)
                .right(rightTable)
                .addAllMatches(columnsToMatch)
                .inverted(inverted)
                .build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return NaturalJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ExactJoinTable exactJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return ExactJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public JoinTable join(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final JoinTable join(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).reserveBits(reserveBits).build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).rule(asOfJoinRule).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return ReverseAsOfJoinTable.builder().left(this).right(rightTable)
                .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).rule(reverseAsOfJoinRule)
                .build();
    }

    @Override
    public final ViewTable view(Collection<? extends Selectable> columns) {
        return ViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(Collection<? extends Selectable> columns) {
        return UpdateViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateTable update(Collection<? extends Selectable> columns) {
        return UpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final LazyUpdateTable lazyUpdate(Collection<? extends Selectable> columns) {
        return LazyUpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final SelectTable select(Collection<? extends Selectable> columns) {
        return SelectTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final AggregateAllTable aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return AggregateAllTable.builder()
                .parent(this)
                .spec(spec)
                .addGroupByColumns(groupByColumns)
                .build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            TableSpec initialGroups, Collection<? extends ColumnName> groupByColumns) {
        return AggregateTable.builder()
                .parent(this)
                .addAllGroupByColumns(groupByColumns)
                .addAllAggregations(aggregations)
                .preserveEmpty(preserveEmpty)
                .initialGroups(Optional.ofNullable(initialGroups))
                .build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return UpdateByTable.builder()
                .parent(this)
                .control(control)
                .addAllOperations(operations)
                .addAllGroupByColumns(byColumns)
                .build();
    }

    @Override
    public final SelectDistinctTable selectDistinct() {
        return SelectDistinctTable.builder().parent(this).build();
    }

    @Override
    public final SelectDistinctTable selectDistinct(Collection<? extends Selectable> columns) {
        return SelectDistinctTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UngroupTable ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return UngroupTable.builder()
                .parent(this)
                .nullFill(nullFill)
                .addAllUngroupColumns(columnsToUngroup)
                .build();
    }

    @Override
    public final DropColumnsTable dropColumns(String... columnNames) {
        final DropColumnsTable.Builder builder = DropColumnsTable.builder()
                .parent(this);
        for (String columnName : columnNames) {
            builder.addDropColumns(ColumnName.of(columnName));
        }
        return builder.build();
    }

    @Override
    public final <V extends TableSchema.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        // the auto-generated toString methods aren't very useful; and being recursive, they can
        // cause stack overflow exceptions that hide other errors in unit tests
        return super.toString();
    }

    private static Collection<String> split(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
                : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
    }
}
