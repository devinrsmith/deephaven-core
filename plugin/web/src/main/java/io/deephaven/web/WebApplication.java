package io.deephaven.web;

import com.google.auto.service.AutoService;
import io.deephaven.appmode.ApplicationState;

import java.io.IOException;
import java.io.UncheckedIOException;

@AutoService(ApplicationState.Factory.class)
public final class WebApplication implements ApplicationState.Factory {

    @Override
    public ApplicationState create(ApplicationState.Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, WebApplication.class.getName(), "Web Application");
        config(state);
        try {
            layout(state);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return state;
    }

    private static void config(ApplicationState state) {
        state.setField("config", WebConfig.systemPropertiesInstance());
    }

    private static void layout(ApplicationState state) throws IOException {
        final WebLayout layout = WebLayout.systemPropertiesInstance().orElse(null);
        if (layout == null) {
            return;
        }
        state.setField("layout", layout);
    }
}
