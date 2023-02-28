package io.deephaven.benchmark;

import io.deephaven.base.clock.Clock;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.table.Table;
import io.deephaven.parquet.table.ParquetTools;

import java.util.Objects;

public class DollarEdgeBenchmark {
    // trades =
    // io.deephaven.parquet.table.ParquetTools.readTable(io.deephaven.configuration.DataDir.get().resolve("trades.parquet").toFile())

    public static void run() {
        final Table trades = ParquetTools.readTable(DataDir.get().resolve("trades.parquet").toFile());
        final Table quotes = ParquetTools.readTable(DataDir.get().resolve("quotes.parquet").toFile());

        final DollarEdgeBenchmark deb = new DollarEdgeBenchmark(trades, quotes);
        try (final ReportExecutor e = ReportExecutor.of("test", Clock.system())) {
            deb.execute(e);
            for (Metric metric : e.metrics()) {
                metric.walk(MetricVisitorPrinter.INSTANCE);
            }
        }

    }


    private final Table trades;
    private final Table quotes;

    public DollarEdgeBenchmark(Table trades, Table quotes) {
        this.trades = Objects.requireNonNull(trades);
        this.quotes = Objects.requireNonNull(quotes);
    }

    private Table selectTrades() {
        return trades.select("Sym", "Timestamp", "Price", "Size");
    }

    private Table selectQuotes() {
        return quotes.select("Sym", "Timestamp", "Mid=(Bid+Ask)/2");
    }

    private Table aj(Table trades, Table quotes) {
        return trades.aj(quotes, "Sym,Timestamp");
    }

    private Table sumBy(Table aj) {
        return aj
                .view("Sym", "DollarEdge=abs(Price-Mid)*Size")
                .sumBy("Sym")
                .sort("Sym");
    }

    public Table execute(ReportExecutor e) {
        final Table trades = e.time("select-trades", this::selectTrades);
        final Table quotes = e.time("select-quotes", this::selectQuotes);
        final Table aj = e.time("aj", () -> aj(trades, quotes));
        return e.time("sum-by", () -> sumBy(aj));
    }
}
