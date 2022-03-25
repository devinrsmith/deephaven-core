package io.deephaven;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.impl.perf.SomeHotspotThing;
import sun.management.ManagementFactoryHelper;

@AutoService(SomeHotspotThing.class)
public class MyImpl implements SomeHotspotThing {

    @Override
    public long getSafepointCount() {
        return ManagementFactoryHelper.getVMManagement().getSafepointCount();
    }
}
