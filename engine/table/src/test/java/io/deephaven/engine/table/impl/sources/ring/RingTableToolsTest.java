package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.emptyTable;

public class RingTableToolsTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void ringFromStaticTable() {

        //TstUtils.assertTableEquals();
        //TstUtils.prevTable();
        //TstUtils.addToTable();

        final Table k = emptyTable(128).view(
                "K_byte=(byte)k",
                "K_char=(char)k",
                "K_double=(double)k",
                "K_float=(float)k",
                "K_int=(int)k",
                "K_long=k",
                "K_short=(short)k",
                "K_str=``+k",
                "K_datetime=new DateTime(k)");
        for (int capacity = 1; capacity <= 256; ++capacity) {
            final Table tail = k.tail(capacity);
            final Table ring = RingTableTools.of(k, capacity, true);
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> TstUtils.assertTableEquals(tail, ring));
        }
    }
}
