/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Character} type.
 */
@Immutable
@SimpleStyle
public abstract class CharType extends PrimitiveTypeBase<Character> {

    public static CharType instance() {
        return ImmutableCharType.of();
    }

    @Override
    public final Class<Character> clazz() {
        return char.class;
    }

    @Override
    public final Class<Character> boxedClass() {
        return Character.class;
    }

    @Override
    public final NativeArrayType<char[], Character> arrayType() {
        return NativeArrayType.of(char[].class, this);
    }

    @Override
    public final NativeArrayType<Character[], Character> boxedArrayType() {
        return NativeArrayType.of(Character[].class, this);
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return CharType.class.getName();
    }
}
