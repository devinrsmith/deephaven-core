package io.deephaven.engine.table.impl;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.util.InverseRowRedirectionImpl;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * The core of the {@link TableWithDefaults#updateBy(UpdateByControl, Collection, MatchPair...)} operation.
 */
public abstract class UpdateBy {
    protected final ChunkSource.WithPrev<Values>[] inputSources;
    protected final int[] inputSourceSlots;
    protected final UpdateByOperator[] operators;
    protected final QueryTable source;
    @Nullable
    protected final WritableRowRedirection redirectionRowSet;
    protected final WritableRowSet freeRows;
    protected long maxInnerIndex;

    protected final UpdateByControl control;

    protected UpdateBy(@NotNull final UpdateByOperator[] operators,
                       @NotNull final QueryTable source,
                       @Nullable final WritableRowRedirection redirectionIndex, UpdateByControl control) {
        this.control = control;
        if(operators.length == 0) {
            throw new IllegalArgumentException("At least one operator must be specified");
        }

        this.source = source;
        this.operators = operators;
        //noinspection unchecked
        this.inputSources = new ChunkSource.WithPrev[operators.length];

        final TObjectIntHashMap<ChunkSource<Values>> sourceToSlotMap = new TObjectIntHashMap<>();
        this.inputSourceSlots = new int[operators.length];
        for(int opIdx = 0; opIdx < operators.length; opIdx++) {
            final ColumnSource<?> input = source.getColumnSource(operators[opIdx].getInputColumnName());
            final int maybeExistingSlot = sourceToSlotMap.get(input);
            if(maybeExistingSlot == sourceToSlotMap.getNoEntryValue()) {
                inputSourceSlots[opIdx] = opIdx;
                sourceToSlotMap.put(input, opIdx);
                inputSources[opIdx] = ReinterpretUtils.maybeConvertToPrimitive(input);
            } else {
                inputSourceSlots[opIdx] = maybeExistingSlot;
            }
        }
        this.redirectionRowSet = redirectionIndex;
        this.freeRows = redirectionIndex == null ? null : RowSetFactory.empty();
    }

    // region UpdateBy implementation
    /**
     * Apply the specified operations to each group of rows in the source table and produce a result table with the same
     * index as the source with each operator applied.
     * 
     * @param source the source to apply to.
     * @param clauses the operations to apply.
     * @param byColumns the columns to group by before applying operations
     *                  
     * @return a new table with the same index as the source with all the operations applied.
     */
    public static Table updateBy(@NotNull final QueryTable source,
                                 @NotNull final Collection<UpdateByClause> clauses,
                                 @NotNull final MatchPair[] byColumns,
                                 @NotNull final UpdateByControl control) {

        WritableRowRedirection redirectionIndex = null;
        if(control.useRedirection()) {
            if(!source.isRefreshing()) {
                if(!source.isFlat() && SparseConstants.sparseStructureExceedsOverhead(source.getRowSet(), control.getMaxStaticSparseMemoryOverhead())) {
                    redirectionIndex = new InverseRowRedirectionImpl(source.getRowSet());
                }
            } else {
                final JoinControl.RedirectionType type = JoinControl.getRedirectionType(source, 4.0, true);
                switch (type) {
                    case Sparse:
                        redirectionIndex = new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
                        break;
                    case Hash:
                        redirectionIndex = WritableRowRedirection.FACTORY.createRowRedirection(source.intSize());
                        break;

                    default:
                        throw new IllegalStateException("Unsupported redirection type " + type);
                }
            }
        }

        final UpdateByOperatorFactory updateByOperatorFactory = new UpdateByOperatorFactory(source, byColumns, redirectionIndex, control);
        final Collection<UpdateByOperator> ops = updateByOperatorFactory.getOperators(clauses);

        final StringBuilder descriptionBuilder = new StringBuilder("updateBy(ops={")
                .append(updateByOperatorFactory.describe(clauses))
                .append("}");

        final Set<String> problems = new LinkedHashSet<>();
        //noinspection rawtypes
        final Map<String, ColumnSource<?>> opResultSources = new LinkedHashMap<>();
        ops.forEach(op -> op.getOutputColumns().forEach((name, col) -> {
            if(opResultSources.putIfAbsent(name, col) != null) {
                problems.add(name);
            }
        }));

        if(!problems.isEmpty()) {
            throw new UncheckedTableException("Multiple Operators tried to produce the same output columns {" +
                String.join(", ", problems) + "}");
        }

        //noinspection rawtypes
        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>(source.getColumnSourceMap());
        resultSources.putAll(opResultSources);

        final UpdateByOperator[] opArr = ops.toArray(UpdateByOperator.ZERO_LENGTH_OP_ARRAY);
        if(byColumns.length == 0) {
            descriptionBuilder.append(")");
            return ZeroKeyUpdateBy.compute(
                    descriptionBuilder.toString(),
                    source,
                    opArr,
                    resultSources,
                    redirectionIndex,
                    control);
        }

        descriptionBuilder.append(", byColumns={").append(MatchPair.matchString(byColumns)).append("})");
        final List<ColumnSource<?>> keySources = new ArrayList<>(byColumns.length);
        for(final MatchPair byColumn : byColumns) {
            if(!source.hasColumns(byColumn.rightColumn)) {
                problems.add(byColumn.rightColumn);
                continue;
            }
            keySources.add(ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(byColumn.rightColumn)));
        }

        if(!problems.isEmpty()) {
            throw new UncheckedTableException(descriptionBuilder + ": Missing byColumns in parent table {" +
                    String.join(", ", problems) + "}");
        }

        return BucketedUpdateBy.compute(descriptionBuilder.toString(),
                source,
                opArr,
                resultSources,
                redirectionIndex,
                keySources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY),
                byColumns,
                control);
    }

    protected void processUpdateForRedirection(@NotNull final TableUpdate upstream) {
        if(upstream.removed().isNonempty()) {
            final RowSetBuilderSequential freeBuilder = RowSetFactory.builderSequential();
            upstream.removed().forAllRowKeys(key -> freeBuilder.appendKey(redirectionRowSet.remove(key)));
            freeRows.insert(freeBuilder.build());
        }

        if(upstream.shifted().nonempty()) {
            try(final WritableRowSet prevIndexLessRemoves = source.getRowSet().copyPrev()) {
                prevIndexLessRemoves.remove(upstream.removed());
                final RowSet.SearchIterator fwdIt = prevIndexLessRemoves.searchIterator();

                upstream.shifted().apply((start, end, delta) -> {
                    if(delta < 0 && fwdIt.advance(start)) {
                        for (long key = fwdIt.currentValue(); fwdIt.currentValue() <= end; key = fwdIt.nextLong()) {
                            if (shiftRedirectedKey(fwdIt, delta, key)) break;
                        }
                    } else {
                        try(final RowSet.SearchIterator revIt = prevIndexLessRemoves.reverseIterator()) {
                            if(revIt.advance(end)) {
                                for (long key = revIt.currentValue(); revIt.currentValue() >= start; key = revIt.nextLong()) {
                                    if (shiftRedirectedKey(revIt, delta, key)) break;
                                }
                            }
                        }
                    }
                });
            }
        }

        if(upstream.added().isNonempty()) {
            final MutableLong lastAllocated = new MutableLong(0);
            final WritableRowSet.Iterator freeIt = freeRows.iterator();
            upstream.added().forAllRowKeys(outerKey -> {
                final long innerKey = freeIt.hasNext() ? freeIt.nextLong() : ++maxInnerIndex;
                lastAllocated.setValue(innerKey);
                redirectionRowSet.put(outerKey, innerKey);
            });
            freeRows.removeRange(0, lastAllocated.longValue());
        }
    }

    private boolean shiftRedirectedKey(@NotNull final RowSet.SearchIterator iterator, final long delta, final long key) {
        final long inner = redirectionRowSet.remove(key);
        if (inner != NULL_ROW_KEY) {
            redirectionRowSet.put(key + delta, inner);
        }
        return !iterator.hasNext();
    }

    /**
     * The type of update to be applied.  For use with invocations of {@link UpdateByOperator#initializeFor(UpdateByOperator.UpdateContext, RowSet, UpdateType)}
     * and {@link UpdateByOperator#finishFor(UpdateByOperator.UpdateContext, UpdateType)}
     */
    public enum UpdateType {
        /** Indicates that rows are being {@link UpdateByOperator#addChunk(UpdateByOperator.UpdateContext, RowSequence, LongChunk, Chunk, long)}  added} to the operator. */
        Add,

        /** Indicates that rows are being {@link UpdateByOperator#removeChunk(UpdateByOperator.UpdateContext, LongChunk, Chunk, long) removed} from the operator. */
        Remove,

        /** Indicates that rows are being {@link UpdateByOperator#modifyChunk(UpdateByOperator.UpdateContext, LongChunk, LongChunk, Chunk, Chunk, long) modified} within the operator */
        Modify,

        /** Indicates that rows are being {@link UpdateByOperator#applyShift(UpdateByOperator.UpdateContext, RowSet, RowSetShiftData) shifted} within the operator. */
        Shift,

        /**
         * Indicates that the {@link TableUpdate} has been processed and rows are
         * being revisited based upon the requests of individual operators for the purposes
         * of doing recalculations on large portions of the table
         */
        Reprocess
    }
    //endregion
}
