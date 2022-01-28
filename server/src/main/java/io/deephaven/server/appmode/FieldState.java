package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.appmode.Field;
import io.deephaven.plugin.app.State;
import io.deephaven.server.object.TypeLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;

final class FieldState implements Listener, State {

    public static FieldState create(TypeLookup typeLookup) {
        return new FieldState(FieldTable.create(), typeLookup);
    }

    private final FieldTable table;
    private final TypeLookup typeLookup;

    private FieldState(FieldTable table, TypeLookup typeLookup) {
        this.table = Objects.requireNonNull(table);
        this.typeLookup = Objects.requireNonNull(typeLookup);
    }

    @Override
    public void onNewField(ApplicationState app, Field<?> field) {
        final Optional<String> type = typeLookup.type(field.value());
        try {
            table.add(app.id(), field.name(), type.orElse(null), field.description().orElse(null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onRemoveField(ApplicationState app, Field<?> field) {
        try {
            table.delete(app.id(), field.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("fields", table(),
                "Application Fields [Id: STRING, Field: STRING, Type: STRING, Description: STRING]");
    }
}
