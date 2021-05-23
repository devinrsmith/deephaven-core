package io.deephaven.qst;

import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ColumnHeaders3<A, B, C> {

    public abstract ColumnHeaders2<A, B> others();

    public abstract ColumnHeader<C> headerC();

    public final <D> ColumnHeaders4<A, B, C, D> header(String name, Class<D> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <D> ColumnHeaders4<A, B, C, D> header(ColumnHeader<D> header) {
        return ImmutableColumnHeaders4.<A, B, C, D>builder()
          .others(this)
          .headerD(header)
          .build();
    }

    public final ColumnHeader<A> headerA() {
        return others().headerA();
    }

    public final ColumnHeader<B> headerB() {
        return others().headerB();
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(headerA(), headerB(), headerC());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c) {
        return start().row(a, b, c);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders2<A, B>.Rows others;
        private final ImmutableColumn.Builder<C> builder;

        Rows() {
            others = others().start();
            builder = ImmutableColumn.<C>builder().header(headerC());
        }

        public final Rows row(A a, B b, C c) {
            others.row(a, b);
            builder.addValues(c);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
