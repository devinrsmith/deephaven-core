package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.agg.key.Key;
import io.deephaven.api.agg.key.KeyAbsSum;
import io.deephaven.api.agg.key.KeyAvg;
import io.deephaven.api.agg.key.KeyFirst;
import io.deephaven.api.agg.key.KeyGroup;
import io.deephaven.api.agg.key.KeyLast;
import io.deephaven.api.agg.key.KeyMax;
import io.deephaven.api.agg.key.KeyMedian;
import io.deephaven.api.agg.key.KeyMin;
import io.deephaven.api.agg.key.KeyPct;
import io.deephaven.api.agg.key.KeyStd;
import io.deephaven.api.agg.key.KeySum;
import io.deephaven.api.agg.key.KeyVar;
import io.deephaven.api.agg.key.KeyWAvg;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest.Aggregate;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ComboAggregateGrpcImpl extends GrpcTableOperation<ComboAggregateRequest> {

    @Inject
    public ComboAggregateGrpcImpl() {
        super(BatchTableRequest.Operation::getComboAggregate, ComboAggregateRequest::getResultId,
                ComboAggregateRequest::getSourceId);
    }

    @Override
    public void validateRequest(ComboAggregateRequest request) throws StatusRuntimeException {
        if (request.getAggregatesCount() == 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "ComboAggregateRequest incorrectly has zero aggregates provided");
        }
        for (Aggregate agg : request.getAggregatesList()) {
            checkColumnName(agg);
            checkPercentile(agg);
            checkAvgMedian(agg);
        }
        if (isSimpleAggregation(request)) {
            // this is a simple aggregation, make sure the user didn't mistakenly set extra properties
            // which would suggest they meant to set force_combo=true
            Aggregate aggregate = request.getAggregates(0);
            if (aggregate.getMatchPairsCount() != 0) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "force_combo is false and only one aggregate provided, but match_pairs is specified");
            }
        }
    }

    private static boolean isSimpleAggregation(ComboAggregateRequest request) {
        return !request.getForceCombo()
                && request.getAggregatesCount() == 1
                && request.getAggregates(0).getMatchPairsCount() == 0;
    }

    @Override
    public Table create(final ComboAggregateRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table parent = sourceTables.get(0).get();
        final Table result;
        if (isSimpleAggregation(request)) {
            // This is a special case with a special operator that can be invoked right off of the table api.
            result = singleAggregateHelper(parent, request.getGroupByColumnsList(), request.getAggregates(0));
        } else {
            result = comboAggregateHelper(parent, request.getGroupByColumnsList(), request.getAggregatesList());
        }
        return result;
    }

    private static Table singleAggregateHelper(Table parent, List<String> groupBySpecs, ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case COUNT:
                return parent.countBy(agg.getColumnName(), groupBySpecs);
            default:
                return parent.aggAllBy(key(agg), groupBySpecs.toArray(String[]::new));
        }
    }

    private static Table comboAggregateHelper(Table parent, List<String> groupBySpecs, List<ComboAggregateRequest.Aggregate> aggregates) {
        final List<Aggregation> aggs =
                aggregates.stream().map(ComboAggregateGrpcImpl::adapt).collect(Collectors.toList());
        return parent.aggBy(aggs, groupBySpecs.toArray(String[]::new));
    }

    private static Aggregation adapt(ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case COUNT:
                return Count.of(agg.getColumnName());
            default:
                final List<Pair> pairs = agg.getMatchPairsList().stream().map(Pair::parse).collect(Collectors.toList());
                return key(agg).aggregation(pairs);
        }
    }

    private static Key key(ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case SUM:
                return KeySum.of();
            case ABS_SUM:
                return KeyAbsSum.of();
            case GROUP:
                return KeyGroup.of();
            case AVG:
                return KeyAvg.of();
            case COUNT:
                throw new IllegalArgumentException("COUNT does not have a key");
            case FIRST:
                return KeyFirst.of();
            case LAST:
                return KeyLast.of();
            case MIN:
                return KeyMin.of();
            case MAX:
                return KeyMax.of();
            case MEDIAN:
                return KeyMedian.of(agg.getAvgMedian());
            case PERCENTILE:
                return KeyPct.of(agg.getPercentile(), agg.getAvgMedian());
            case STD:
                return KeyStd.of();
            case VAR:
                return KeyVar.of();
            case WEIGHTED_AVG:
                return KeyWAvg.of(ColumnName.of(agg.getColumnName()));
            default:
                throw new UnsupportedOperationException("Unexpected aggregation: " + agg.getType());
        }
    }

    private static void checkColumnName(ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case COUNT:
            case WEIGHTED_AVG:
                if (agg.getColumnName().isEmpty()) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "column_name must be specified for type " + agg.getType());
                }
                return;
        }
        if (!agg.getColumnName().isEmpty()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "column_name is specified for type " + agg.getType());
        }
    }

    private static void checkAvgMedian(ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case MEDIAN:
            case PERCENTILE:
                return;
        }
        if (agg.getAvgMedian()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "avg_median is specified for type " + agg.getType());
        }
    }

    private static void checkPercentile(ComboAggregateRequest.Aggregate agg) {
        switch (agg.getType()) {
            case PERCENTILE:
                return;
        }
        if (agg.getPercentile() != 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "percentile is specified for type " + agg.getType());
        }
    }
}
