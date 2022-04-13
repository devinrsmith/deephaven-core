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
                "K_datetime=new DateTime(k)",
                "K_boolean=k%3==0?null:k%3==1");
        for (int capacity = 1; capacity <= 256; ++capacity) {
            final Table tail = k.tail(capacity);
            final Table ring = RingTableTools.of(k, capacity, true);
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                checkEquals(tail, ring);
            });
        }
    }

    private void checkEquals(Table tail, Table ring) {
        TstUtils.assertTableEquals(tail, ring);

        // dense indices
        TstUtils.assertTableEquals(tail.where("i%2!=0"), ring.where("i%2!=0"));
        TstUtils.assertTableEquals(tail.where("i%3!=0"), ring.where("i%3!=0"));
        TstUtils.assertTableEquals(tail.where("i%5!=0"), ring.where("i%5!=0"));
        TstUtils.assertTableEquals(tail.where("i%7!=0"), ring.where("i%7!=0"));
        TstUtils.assertTableEquals(tail.where("i%11!=0"), ring.where("i%11!=0"));

        // sparse indices
        TstUtils.assertTableEquals(tail.where("i%2==0"), ring.where("i%2==0"));
        TstUtils.assertTableEquals(tail.where("i%3==0"), ring.where("i%3==0"));
        TstUtils.assertTableEquals(tail.where("i%5==0"), ring.where("i%5==0"));
        TstUtils.assertTableEquals(tail.where("i%7==0"), ring.where("i%7==0"));
        TstUtils.assertTableEquals(tail.where("i%11==0"), ring.where("i%11==0"));
    }
}
