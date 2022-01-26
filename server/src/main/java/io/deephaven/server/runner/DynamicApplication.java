package io.deephaven.server.runner;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppBase;

@AutoService(App.class)
public final class DynamicApplication extends AppBase {

    public DynamicApplication() {
        super(DynamicApplication.class.getName(), "An example of a 'dynamic' application");
    }

    @Override
    public void insertInto(Consumer consumer) {
        new Thread(() -> {
            for (int i = 0;; ++i) {
                consumer.set(String.format("item_%d", i), i, String.format("Item %d", i));
                try {
                    Thread.sleep(i * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "DynamicApplication").start();
    }
}
