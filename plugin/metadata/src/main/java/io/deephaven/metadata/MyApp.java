package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppStates;

import java.time.Instant;

@AutoService(App.class)
public final class MyApp extends AppStates {

    private static final TheMetadata INSTANCE = new TheMetadata("my version", Instant.now());

    public MyApp() {
        super(MyApp.class.getName(), "MyApp", INSTANCE);
    }
}
