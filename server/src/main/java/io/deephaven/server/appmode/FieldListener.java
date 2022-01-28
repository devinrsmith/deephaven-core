package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.appmode.Field;
import io.deephaven.server.object.TypeLookup;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;

final class FieldListener implements Listener {

    private final FieldTable table;
    private final TypeLookup typeLookup;

    @Inject
    public FieldListener(FieldTable table, TypeLookup typeLookup) {
        this.table = Objects.requireNonNull(table);
        this.typeLookup = Objects.requireNonNull(typeLookup);
    }

    public FieldTable table() {
        return table;
    }

    @Override
    public void onNewField(ApplicationState app, Field<?> field) {
        // todo: do we need         if (!mode.hasVisibilityToAppExports()) {
        //            return;
        //        }
        // guards?
        final Optional<String> type = typeLookup.type(field.value());
        try {
            table.add(app.id(), field.name(), type.orElse(null), field.description().orElse(null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onReplaceField(ApplicationState app, Field<?> oldField, Field<?> field) {
        // todo: do we need guards
        final Optional<String> type = typeLookup.type(field.value());
        try {
            table.add(app.id(), field.name(), type.orElse(null), field.description().orElse(null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onRemoveField(ApplicationState app, Field<?> field) {
        // todo: do we need guards
        try {
            table.delete(app.id(), field.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
