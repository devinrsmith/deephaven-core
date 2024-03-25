//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import java.util.Objects;

/**
 * An array-like type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 * @see NativeArrayType
 * @see PrimitiveVectorType
 * @see GenericVectorType
 */
public interface ArrayType<T, ComponentType> extends GenericType<T> {

    static <T, ComponentType> ArrayType<T, ComponentType> find(Class<T> clazz,
            Class<ComponentType> componentTypeClass) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(componentTypeClass);
        if (clazz.isArray()) {
            return NativeArrayType.of(clazz, Type.find(componentTypeClass));
        }
        if (PrimitiveVectorType.isPrimitiveVectorType(clazz)) {
            return PrimitiveVectorType.of(clazz, PrimitiveType.find(componentTypeClass));
        }
        if (GenericVectorType.isGenericVectorType(clazz)) {
            return GenericVectorType.of(clazz, GenericType.find(componentTypeClass));
        }
        throw new IllegalArgumentException(String.format("Class '%s' is not an array type", clazz.getName()));
    }

    /**
     * The component type.
     *
     * @return the component type
     */
    Type<ComponentType> componentType();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(NativeArrayType<?, ?> nativeArrayType);

        R visit(PrimitiveVectorType<?, ?> vectorPrimitiveType);

        R visit(GenericVectorType<?, ?> genericVectorType);
    }
}
