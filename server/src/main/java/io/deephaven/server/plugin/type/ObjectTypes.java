package io.deephaven.server.plugin.type;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.app.App.State;
import io.deephaven.plugin.app.App.Consumer;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.plugin.type.ObjectTypeRegistration;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.lang.model.SourceVersion;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provides synchronized object type {@link ObjectTypeRegistration registration} and {@link ObjectTypeLookup lookup}.
 *
 * <p>
 * Object type registration that is an instances of {@link ObjectTypeClassBase} receives special consideration, and
 * these objects have more efficient lookups.
 */
@Singleton
public final class ObjectTypes implements ObjectTypeLookup, ObjectTypeRegistration, State {

    private static final Set<String> RESERVED_TYPE_NAMES_LOWERCASE = Set.of("table", "tablemap", "treetable", "");

    private final Set<String> namesLowercase;
    private final Map<Class<?>, ObjectType> classTypes;
    private final List<ObjectType> otherTypes;
    private final ObjectTypesTable table;

    @Inject
    public ObjectTypes() {
        namesLowercase = new HashSet<>();
        classTypes = new HashMap<>();
        otherTypes = new ArrayList<>();
        table = ObjectTypesTable.create();
    }

    @Override
    public synchronized Optional<ObjectType> findObjectType(Object object) {
        final ObjectType byClass = classTypes.get(object.getClass());
        if (byClass != null) {
            return Optional.of(byClass);
        }
        for (ObjectType type : otherTypes) {
            if (type.isType(object)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized void register(ObjectType objectType) {
        final String name = objectType.name();
        final String nameLowercase = name.toLowerCase(Locale.ENGLISH);
        if (SourceVersion.isKeyword(nameLowercase)) {
            throw new IllegalArgumentException("Unable to register type, name is keyword: " + name);
        }
        if (isReservedName(nameLowercase)) {
            throw new IllegalArgumentException("Unable to register type, name is reserved: " + name);
        }
        if (namesLowercase.contains(nameLowercase)) {
            throw new IllegalArgumentException(
                    "Unable to register type, type name already registered: " + name);
        }
        if (objectType instanceof ObjectTypeClassBase) {
            final Class<?> clazz = ((ObjectTypeClassBase<?>) objectType).clazz();
            if (classTypes.putIfAbsent(clazz, objectType) != null) {
                throw new IllegalArgumentException("Unable to register type, class already registered: " + clazz);
            }
        } else {
            otherTypes.add(objectType);
        }
        namesLowercase.add(nameLowercase);
        try {
            table.add(objectType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("objectTypeNames", objectTypeNames());
        consumer.set("reservedNames", reservedNames());
    }

    private Table objectTypeNames() {
        return table.table();
    }

    private static Table reservedNames() {
        return InMemoryTable.from(NewTable.of(Column.of("Name", String.class, RESERVED_TYPE_NAMES_LOWERCASE)));
    }

    private static boolean isReservedName(String name) {
        return RESERVED_TYPE_NAMES_LOWERCASE.contains(name);
    }
}
