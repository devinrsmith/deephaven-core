package io.deephaven.api.literal;

import io.deephaven.api.literal.Literal.Visitor;

public enum AsObject implements Visitor<Object> {
    INSTANCE;

    public static Object of(Literal literal) {
        return literal.walk(INSTANCE);
    }

    @Override
    public Object visit(boolean literal) {
        return literal;
    }

    @Override
    public Object visit(char literal) {
        return literal;
    }

    @Override
    public Object visit(byte literal) {
        return literal;
    }

    @Override
    public Object visit(short literal) {
        return literal;
    }

    @Override
    public Object visit(int literal) {
        return literal;
    }

    @Override
    public Object visit(long literal) {
        return literal;
    }

    @Override
    public Object visit(float literal) {
        return literal;
    }

    @Override
    public Object visit(double literal) {
        return literal;
    }

    @Override
    public Object visit(String literal) {
        return literal;
    }
}
