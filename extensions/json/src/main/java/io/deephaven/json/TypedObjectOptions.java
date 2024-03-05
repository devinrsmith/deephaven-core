/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * A type-discriminated object.
 */
@Immutable
@BuildableStyle
public abstract class TypedObjectOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableTypedObjectOptions.builder();
    }

    public abstract String typeFieldName();

    public abstract Map<String, ValueOptions> sharedFields();

    public abstract Map<String, ObjectOptions> objects();

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    @Default
    public boolean allowUnknownTypes() {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<TypedObjectOptions, Builder> {

        Builder typeFieldName(String typeFieldName);

        Builder putSharedFields(String key, ValueOptions value);

        Builder putSharedFields(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllSharedFields(Map<String, ? extends ValueOptions> entries);

        Builder putObjects(String key, ObjectOptions value);

        Builder putObjects(Map.Entry<String, ? extends ObjectOptions> entry);

        Builder putAllObjects(Map<String, ? extends ObjectOptions> entries);

        Builder allowUnknownTypes(boolean allowUnknownTypes);
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }
}
