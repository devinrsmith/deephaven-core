package io.deephaven.appmode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ApplicationState {

    public interface Factory {
        ApplicationState create(Set<Listener> appStateListeners);
    }

    public interface Listener {
        void onNewField(ApplicationState app, Field<?> field);

        void onReplaceField(ApplicationState app, Field<?> oldField, Field<?> field);

        void onRemoveField(ApplicationState app, Field<?> field);
    }

    private final Set<Listener> listeners;
    private final String id;
    private final String name;
    private final Map<String, Field<?>> fields;

    public ApplicationState(Set<Listener> listeners, String id, String name) {
        this.listeners = Objects.requireNonNull(listeners);
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.fields = new HashMap<>();
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public synchronized int numFieldsExported() {
        return fields.size();
    }

    public synchronized List<Field<?>> listFields() {
        return new ArrayList<>(fields.values());
    }

    public synchronized void clearFields() {
        for (Field<?> field : fields.values()) {
            for (Listener listener : listeners) {
                listener.onRemoveField(this, field);
            }
        }
        fields.clear();
    }

    public synchronized <T> Field<T> getField(String name) {
        // noinspection unchecked
        return (Field<T>) fields.get(name);
    }

    public synchronized <T> void setField(String name, T value) {
        setField(StandardField.of(name, value));
    }

    public synchronized <T> void setField(String name, T value, String description) {
        setField(StandardField.of(name, value, description));
    }

    public synchronized void setField(Field<?> field) {
        Field<?> oldField = fields.remove(field.name());
        if (oldField != null) {
            for (Listener listener : listeners) {
                listener.onReplaceField(this, oldField, field);
            }
        } else {
            for (Listener listener : listeners) {
                listener.onNewField(this, field);
            }
        }
        fields.put(field.name(), field);
    }

    public synchronized void setFields(Field<?>... fields) {
        setFields(Arrays.asList(fields));
    }

    public synchronized void setFields(Iterable<Field<?>> fields) {
        for (Field<?> field : fields) {
            setField(field);
        }
    }

    public synchronized void removeField(String name) {
        Field<?> field = fields.remove(name);
        if (field != null) {
            for (Listener listener : listeners) {
                listener.onRemoveField(this, field);
            }
        }
    }

    public synchronized void removeFields(String... names) {
        removeFields(Arrays.asList(names));
    }

    public synchronized void removeFields(Iterable<String> names) {
        for (String name : names) {
            removeField(name);
        }
    }
}
