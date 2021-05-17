package io.deephaven.datastructures.util;

import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ColumnHeaders4<A, B, C, D> {

    public abstract ColumnHeaders3<A, B, C> others();

    public abstract ColumnHeader<D> headerD();

    public final <E> ColumnHeaders5<A, B, C, D, E> header(String name, Class<E> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <E> ColumnHeaders5<A, B, C, D, E> header(ColumnHeader<E> header) {
        return ImmutableColumnHeaders5.<A, B, C, D, E>builder()
          .others(this)
          .headerE(header)
          .build();
    }

    public final ColumnHeader<A> headerA() {
        return others().headerA();
    }

    public final ColumnHeader<B> headerB() {
        return others().headerB();
    }

    public final ColumnHeader<C> headerC() {
        return others().headerC();
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d) {
        return start().row(a, b, c, d);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders3<A, B, C>.Rows others;
        private final ImmutableColumn.Builder<D> builder;

        Rows() {
            others = others().start();
            builder = ImmutableColumn.<D>builder().header(headerD());
        }

        public final Rows row(A a, B b, C c, D d) {
            others.row(a, b, c);
            builder.addValues(d);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
