package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.spec.CumMinMaxSpec;
import io.deephaven.api.updateby.spec.CumProdSpec;
import io.deephaven.api.updateby.spec.CumSumSpec;
import io.deephaven.api.updateby.spec.EmaSpec;
import io.deephaven.api.updateby.spec.FillBySpec;
import io.deephaven.api.updateby.spec.TimeScale;
import io.deephaven.api.updateby.spec.UpdateBySpec;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMax;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMin;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaOptions;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByFill;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOptions;
import io.deephaven.qst.table.UpdateByTable;
import io.deephaven.qst.table.UpdateByTable.Builder;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.math.MathContext;
import java.math.RoundingMode;

@Singleton
public final class UpdateByGrpcImpl extends GrpcQstTableOperation<UpdateByRequest, UpdateByTable> {

    @Inject
    public UpdateByGrpcImpl() {
        super(BatchTableRequest.Operation::getUpdateBy, UpdateByRequest::getResultId, UpdateByRequest::getSourceId);
    }

    @Override
    UpdateByTable createTableSpec(UpdateByRequest request) {
        return adapt(request);
    }

    @Override
    void validateSecurity(UpdateByTable updateByTable) {
        for (Selectable groupByColumn : updateByTable.groupByColumns()) {
            ExpressionSecurity.validateSecurity(groupByColumn.expression());
        }
    }

    private static UpdateByTable adapt(UpdateByRequest request) {
        final Builder builder = UpdateByTable.builder().parent(GrpcExportTable.of(0));
        if (request.hasOptions()) {
            builder.control(adaptOptions(request.getOptions()));
        }
        for (UpdateByRequest.UpdateByOperation operation : request.getOperationsList()) {
            builder.addOperations(adaptOperation(operation));
        }
        for (String groupByColumn : request.getGroupByColumnsList()) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.build();
    }

    private static UpdateByControl adaptOptions(UpdateByOptions options) {
        UpdateByControl.Builder builder = UpdateByControl.builder();
        if (options.hasUseRedirection()) {
            builder.useRedirection(options.getUseRedirection());
        }
        if (options.hasChunkCapacity()) {
            builder.chunkCapacity(options.getChunkCapacity());
        }
        if (options.hasMaxStaticSparseMemoryOverhead()) {
            builder.maxStaticSparseMemoryOverhead(options.getMaxStaticSparseMemoryOverhead());
        }
        if (options.hasInitialHashTableSize()) {
            builder.initialHashTableSize(options.getInitialHashTableSize());
        }
        if (options.hasMaximumLoadFactor()) {
            builder.maximumLoadFactor(options.getMaximumLoadFactor());
        }
        if (options.hasTargetLoadFactor()) {
            builder.targetLoadFactor(options.getTargetLoadFactor());
        }
        if (options.hasMathContext()) {
            builder.mathContext(adaptMathContext(options.getMathContext()));
        }
        return builder.build();
    }

    private static UpdateByOperation adaptOperation(UpdateByRequest.UpdateByOperation operation) {
        switch (operation.getTypeCase()) {
            case COLUMN:
                return adaptColumn(operation.getColumn());
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected operation type case: " + operation.getTypeCase());
        }
    }

    private static ColumnUpdateOperation adaptColumn(UpdateByColumn column) {
        ColumnUpdateOperation.Builder builder = ColumnUpdateOperation.builder()
                .spec(adaptSpec(column.getSpec()));
        for (io.deephaven.proto.backplane.grpc.Pair pair : column.getPairList()) {
            builder.addColumns(PairBuilder.adapt(pair));
        }
        return builder.build();
    }

    private static UpdateBySpec adaptSpec(UpdateByColumn.UpdateBySpec spec) {
        switch (spec.getTypeCase()) {
            case SUM:
                return adaptSum(spec.getSum());
            case MIN:
                return adaptMin(spec.getMin());
            case MAX:
                return adaptMax(spec.getMax());
            case PRODUCT:
                return adaptProduct(spec.getProduct());
            case FILL:
                return adaptFill(spec.getFill());
            case EMA:
                return adaptEma(spec.getEma());
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected spec type: " + spec.getTypeCase());
        }
    }

    private static CumSumSpec adaptSum(@SuppressWarnings("unused") UpdateByCumulativeSum sum) {
        return CumSumSpec.of();
    }

    private static CumMinMaxSpec adaptMin(@SuppressWarnings("unused") UpdateByCumulativeMin min) {
        return CumMinMaxSpec.of(false);
    }

    private static CumMinMaxSpec adaptMax(@SuppressWarnings("unused") UpdateByCumulativeMax max) {
        return CumMinMaxSpec.of(true);
    }

    private static CumProdSpec adaptProduct(@SuppressWarnings("unused") UpdateByCumulativeProduct product) {
        return CumProdSpec.of();
    }

    private static FillBySpec adaptFill(@SuppressWarnings("unused") UpdateByFill fill) {
        return FillBySpec.of();
    }

    private static EmaSpec adaptEma(UpdateByEma ema) {
        return ema.hasOptions() ? EmaSpec.of(adaptEmaOptions(ema.getOptions()), adaptTimescale(ema.getTimescale()))
                : EmaSpec.of(adaptTimescale(ema.getTimescale()));
    }

    private static OperationControl adaptEmaOptions(UpdateByEmaOptions options) {
        final OperationControl.Builder builder = OperationControl.builder();
        if (options.hasOnNullValue()) {
            builder.onNullValue(adaptBadDataBehavior(options.getOnNullValue()));
        }
        if (options.hasOnNanValue()) {
            builder.onNanValue(adaptBadDataBehavior(options.getOnNanValue()));
        }
        if (options.hasOnNullTime()) {
            builder.onNullTime(adaptBadDataBehavior(options.getOnNullTime()));
        }
        if (options.hasOnNegativeDeltaTime()) {
            builder.onNegativeDeltaTime(adaptBadDataBehavior(options.getOnNegativeDeltaTime()));
        }
        if (options.hasOnZeroDeltaTime()) {
            builder.onZeroDeltaTime(adaptBadDataBehavior(options.getOnZeroDeltaTime()));
        }
        if (options.hasBigValueContext()) {
            builder.bigValueContext(adaptMathContext(options.getBigValueContext()));
        }
        return builder.build();
    }

    private static MathContext adaptMathContext(io.deephaven.proto.backplane.grpc.MathContext bigValueContext) {
        return new MathContext(bigValueContext.getPrecision(), adaptRoundingMode(bigValueContext.getRoundingMode()));
    }

    private static RoundingMode adaptRoundingMode(
            io.deephaven.proto.backplane.grpc.MathContext.RoundingMode roundingMode) {
        switch (roundingMode) {
            case UP:
                return RoundingMode.UP;
            case DOWN:
                return RoundingMode.DOWN;
            case CEILING:
                return RoundingMode.CEILING;
            case FLOOR:
                return RoundingMode.FLOOR;
            case HALF_UP:
                return RoundingMode.HALF_UP;
            case HALF_DOWN:
                return RoundingMode.HALF_DOWN;
            case HALF_EVEN:
                return RoundingMode.HALF_EVEN;
            case UNNECESSARY:
                return RoundingMode.UNNECESSARY;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected rounding mode: " + roundingMode);
        }
    }

    private static TimeScale adaptTimescale(UpdateByEma.UpdateByEmaTimescale timescale) {
        switch (timescale.getTypeCase()) {
            case TICKS:
                return TimeScale.ofTicks(timescale.getTicks().getTicks());
            case TIME:
                return TimeScale.ofTime(timescale.getTime().getColumn(), timescale.getTime().getPeriodNanos());
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected timescale type: " + timescale.getTypeCase());
        }
    }

    private static BadDataBehavior adaptBadDataBehavior(io.deephaven.proto.backplane.grpc.BadDataBehavior b) {
        switch (b) {
            case RESET:
                return BadDataBehavior.RESET;
            case SKIP:
                return BadDataBehavior.SKIP;
            case THROW:
                return BadDataBehavior.THROW;
            case POISON:
                return BadDataBehavior.POISON;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected BadDataBehavior: " + b);
        }
    }
}
