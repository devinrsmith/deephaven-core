package io.deephaven.figure;

import io.deephaven.engine.util.TableTools;
import io.deephaven.plugin.application.ApplicationInfo;

public enum FigureApp implements ApplicationInfo {
    FigureApp;

    @Override
    public String id() {
        return FigureApp.class.getName();
    }

    @Override
    public void initializeInto(State state) {
        state.setField("hello", TableTools.emptyTable(1).view("X=i"));
    }
}
