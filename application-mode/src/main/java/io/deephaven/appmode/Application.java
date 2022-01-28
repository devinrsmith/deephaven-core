package io.deephaven.appmode;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.appmode.ApplicationState.Listener;
import org.immutables.value.Value.Immutable;

import java.util.Set;

@Immutable
@BuildableStyle
public abstract class Application {

    public interface Builder {

        Builder id(String id);

        Builder name(String name);

        Builder fields(Fields fields);

        Application build();
    }

    public interface Factory {
        Application create();
    }

    public static Builder builder() {
        return ImmutableApplication.builder();
    }

    /**
     * The application id, should be unique and unchanging.
     *
     * @return the application id
     */
    public abstract String id();

    /**
     * The application name.
     *
     * @return the application name
     */
    public abstract String name();

    /**
     * The fields.
     *
     * @return the fields
     */
    public abstract Fields fields();

    public final ApplicationState toState(final Set<Listener> appStateListener) {
        final ApplicationState state = new ApplicationState(appStateListener, id(), name());
        state.setFields(fields());
        return state;
    }
}
