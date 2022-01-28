package io.deephaven.server.runner;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppBase;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@AutoService(App.class)
public final class DynamicApplication extends AppBase {

    private final ScheduledExecutorService executor;

    public DynamicApplication() {
        super(DynamicApplication.class.getName(), "An example of a 'dynamic' application");
        executor = Executors.newScheduledThreadPool(1); // note: could be injected if necessary
    }

    @Override
    public void insertInto(Consumer consumer) {
        executor.schedule(new Update(consumer), 0, TimeUnit.SECONDS);
    }

    private class Update implements Runnable {
        private final Consumer consumer;
        private int i;

        public Update(Consumer consumer) {
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void run() {
            consumer.set(String.format("item_%d", i), i, String.format("Item %d", i));
            ++i;
            executor.schedule(this, i, TimeUnit.SECONDS);
        }
    }
}
