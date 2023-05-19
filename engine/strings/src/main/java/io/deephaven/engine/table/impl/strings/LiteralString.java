package io.deephaven.engine.table.impl.strings;

import io.deephaven.api.literal.Literal;
import org.apache.commons.text.StringEscapeUtils;

/**
 * @see io.deephaven.engine.table.impl.strings
 */
public enum LiteralString implements Literal.Visitor<String> {
    INSTANCE;

    public static String of(Literal literal) {
        return literal.walk(INSTANCE);
    }

    @Override
    public String visit(boolean literal) {
        return Boolean.toString(literal);
    }

    @Override
    public String visit(char literal) {
        return "'" + literal + "'";
    }

    @Override
    public String visit(byte literal) {
        return "(byte)" + literal;
    }

    @Override
    public String visit(short literal) {
        return "(short)" + literal;
    }

    @Override
    public String visit(int literal) {
        return "(int)" + literal;
    }

    @Override
    public String visit(long literal) {
        return literal + "L";
    }

    @Override
    public String visit(float literal) {
        return literal + "f";
    }

    @Override
    public String visit(double literal) {
        return literal + "d";
    }

    @Override
    public String visit(String literal) {
        return '"' + StringEscapeUtils.escapeJava(literal) + '"';
    }
}
