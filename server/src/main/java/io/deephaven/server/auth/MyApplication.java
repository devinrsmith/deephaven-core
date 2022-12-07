package io.deephaven.server.auth;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTime;
import io.deephaven.util.SafeCloseable;

import java.util.concurrent.TimeUnit;

public class MyApplication implements ApplicationState.Factory {

    private LivenessScope scope;

    @Override
    public ApplicationState create(Listener appStateListener) {
        final ApplicationState state =
                new ApplicationState(appStateListener, MyApplication.class.getName(), "MyApplication");
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false);
                final SafeCloseable _lock = UpdateGraphProcessor.DEFAULT.sharedLock().lockCloseable()) {
            create(state);
        }
        return state;
    }

    private static void create(ApplicationState state) {
        final Table one = TableTools.emptyTable(1L).view("I=ii");
        final Table thousand = TableTools.emptyTable(1000L).view("I=ii");
        final Table million = TableTools.emptyTable(1000000L).view("I=ii");
        final Table billion = TableTools.emptyTable(1000000000L).view("I=ii");
        final Table trillion = TableTools.emptyTable(1000000000000L).view("I=ii");

        final Table current_time = TableTools.timeTable(TimeUnit.SECONDS.toNanos(1)).lastBy();

        // leaving out some, see https://github.com/deephaven/deephaven-core/issues/3167
        // final Table epoch_1us = TableTools.timeTable(new DateTime(0), TimeUnit.MICROSECONDS.toNanos(1));
        final Table epoch_1ms = TableTools.timeTable(new DateTime(0), TimeUnit.MILLISECONDS.toNanos(1));
        final Table epoch_1s = TableTools.timeTable(new DateTime(0), TimeUnit.SECONDS.toNanos(1));
        final Table epoch_1m = TableTools.timeTable(new DateTime(0), TimeUnit.MINUTES.toNanos(1));
        final Table epoch_1h = TableTools.timeTable(new DateTime(0), TimeUnit.HOURS.toNanos(1));
        final Table epoch_1d = TableTools.timeTable(new DateTime(0), TimeUnit.DAYS.toNanos(1));

        final DateTime now = DateTime.now();
        final Table uptime_1ns = TableTools.timeTable(now, TimeUnit.NANOSECONDS.toNanos(1));
        final Table uptime_1us = TableTools.timeTable(now, TimeUnit.MICROSECONDS.toNanos(1));
        final Table uptime_1ms = TableTools.timeTable(now, TimeUnit.MILLISECONDS.toNanos(1));
        final Table uptime_1s = TableTools.timeTable(now, TimeUnit.SECONDS.toNanos(1));
        final Table uptime_1m = TableTools.timeTable(now, TimeUnit.MINUTES.toNanos(1));
        final Table uptime_1h = TableTools.timeTable(now, TimeUnit.HOURS.toNanos(1));
        final Table uptime_1d = TableTools.timeTable(now, TimeUnit.DAYS.toNanos(1));

        final PartitionedTable pt = PartitionedTableFactory.ofTables(
                one.countBy("Rows").updateView("Table=`one`"),
                thousand.countBy("Rows").updateView("Table=`thousand`"),
                million.countBy("Rows").updateView("Table=`million`"),
                billion.countBy("Rows").updateView("Table=`billion`"),
                trillion.countBy("Rows").updateView("Table=`trillion`"),
                current_time.countBy("Rows").updateView("Table=`current_time`"),
                epoch_1d.countBy("Rows").updateView("Table=`epoch_1d`"),
                epoch_1h.countBy("Rows").updateView("Table=`epoch_1h`"),
                epoch_1m.countBy("Rows").updateView("Table=`epoch_1m`"),
                epoch_1s.countBy("Rows").updateView("Table=`epoch_1s`"),
                epoch_1ms.countBy("Rows").updateView("Table=`epoch_1ms`"),
                uptime_1d.countBy("Rows").updateView("Table=`uptime_1d`"),
                uptime_1h.countBy("Rows").updateView("Table=`uptime_1h`"),
                uptime_1m.countBy("Rows").updateView("Table=`uptime_1m`"),
                uptime_1s.countBy("Rows").updateView("Table=`uptime_1s`"),
                uptime_1ms.countBy("Rows").updateView("Table=`uptime_1ms`"),
                uptime_1us.countBy("Rows").updateView("Table=`uptime_1us`"),
                uptime_1ns.countBy("Rows").updateView("Table=`uptime_1ns`"));

        state.setField("i_one", one);
        state.setField("i_thousand", thousand);
        state.setField("i_million", million);
        state.setField("i_billion", billion);
        state.setField("i_trillion", trillion);

        state.setField("current_time", current_time);

        state.setField("uptime_1ns", uptime_1ns.reverse());
        state.setField("uptime_1us", uptime_1us.reverse());
        state.setField("uptime_1ms", uptime_1ms.reverse());
        state.setField("uptime_1s", uptime_1s.reverse());
        state.setField("uptime_1m", uptime_1m.reverse());
        state.setField("uptime_1h", uptime_1h.reverse());
        state.setField("uptime_1d", uptime_1d.reverse());

        state.setField("epoch_1ms", epoch_1ms.reverse());
        state.setField("epoch_1s", epoch_1s.reverse());
        state.setField("epoch_1m", epoch_1m.reverse());
        state.setField("epoch_1h", epoch_1h.reverse());
        state.setField("epoch_1d", epoch_1d.reverse());

        state.setField("row_counts", pt.merge().moveColumnsDown("Table"));
    }
}
