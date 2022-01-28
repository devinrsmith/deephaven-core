package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;

import java.util.Set;

public class ScriptApplicationState extends ApplicationState {

    private final ScriptSession scriptSession;

    public ScriptApplicationState(final ScriptSession scriptSession,
            final Set<Listener> listeners,
            final String id,
            final String name) {
        super(listeners, id, name);
        this.scriptSession = scriptSession;
    }

    @Override
    public synchronized <T> void setField(String name, T value, String description) {
        super.setField(name, scriptSession.unwrapObject(value), description);
    }

    @Override
    public synchronized <T> void setField(String name, T value) {
        super.setField(name, scriptSession.unwrapObject(value));
    }
}
