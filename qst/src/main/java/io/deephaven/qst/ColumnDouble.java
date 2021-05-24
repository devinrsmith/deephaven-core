package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;

class ColumnDouble {

    public static Column<Double> from(Column<?> other) {
        return other.type().walk(new ToDoubleColumn(other)).getOut();
    }

    static class ToDoubleColumn implements Visitor {
        private final Column<?> in;
        private Column<Double> out;

        public ToDoubleColumn(Column<?> in) {
            this.in = Objects.requireNonNull(in);
        }

        public Column<Double> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            out = ColumnHeader
                .ofDouble(in.name())
                .withData(() -> intType.cast(in).values().stream().map(ToDoubleColumn::toDouble).iterator());
        }

        @Override
        public void visit(StringType stringType) {
            throw new RuntimeException("todo");
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = doubleType.cast(in);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            throw new RuntimeException("todo");
        }

        private static Double toDouble(Integer i) {
            return i == null ? null : (double)i;
        }
    }
}
