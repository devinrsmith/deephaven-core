package io.deephaven.server.runner;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppBase;

@AutoService(App.class)
public final class EmptyApplication extends AppBase {

    public EmptyApplication() {
        super(EmptyApplication.class.getName(), "An example of an 'empty' application");
    }

    @Override
    public void insertInto(Consumer consumer) {

    }
}
