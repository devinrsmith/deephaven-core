package io.deephaven.qst;

import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ColumnHeaders5<A, B, C, D, E> {

    public abstract ColumnHeaders4<A, B, C, D> others();

    public abstract ColumnHeader<E> headerE();

    public final ColumnHeader<A> headerA() {
        return others().headerA();
    }

    public final ColumnHeader<B> headerB() {
        return others().headerB();
    }

    public final ColumnHeader<C> headerC() {
        return others().headerC();
    }

    public final ColumnHeader<D> headerD() {
        return others().headerD();
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e) {
        return start().row(a, b, c, d, e);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders4<A, B, C, D>.Rows others;
        private final ImmutableColumn.Builder<E> builder;

        Rows() {
            others = others().start();
            builder = ImmutableColumn.<E>builder().header(headerE());
        }

        public final Rows row(A a, B b, C c, D d, E e) {
            others.row(a, b, c, d);
            builder.addValues(e);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
