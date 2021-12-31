package io.deephaven.server.plugin;

import io.deephaven.plugin.application.ApplicationInfo.State;
import org.jpy.PyObject;

import java.util.Objects;

// TODO(deephaven-core#1775): multivariate jpy (unwrapped) type conversion into java
public class Forker {
    private final State state;

    public Forker(State state) {
        this.state = Objects.requireNonNull(state);
    }

//        @SuppressWarnings("unused")
//        public void set_field(String name, PyObject object) {
//            // TODO(deephaven-core#1775): multivariate jpy (unwrapped) type conversion into java
//            final Object unwrapped = module.unwrap_to_java_type(object);
//            if (unwrapped != null) {
//                state.setField(name, unwrapped);
//            } else {
//                state.setField(name, object);
//            }
//        }

    @SuppressWarnings("unused")
    public void setFieldPython(String name, PyObject object) {
        state.setField(name, object);
    }

    @SuppressWarnings("unused")
    public void setFieldJava(String name, Object object) {
        state.setField(name, object);
    }
}
