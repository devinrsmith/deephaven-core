package io.deephaven.qst;

import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders2<A, B> {

    @Parameter
    public abstract ColumnHeader<A> headerA();

    @Parameter
    public abstract ColumnHeader<B> headerB();

    public final <C> ColumnHeaders3<A, B, C> header(String name, Class<C> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <C> ColumnHeaders3<A, B, C> header(String name, ColumnType<C> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <C> ColumnHeaders3<A, B, C> header(ColumnHeader<C> header) {
        return ImmutableColumnHeaders3.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.of(headerA(), headerB());
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b) {
        return start().row(a, b);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeader<A>.Rows others;
        private final ImmutableColumn.Builder<B> builder;

        Rows() {
            others = headerA().start();
            builder = ImmutableColumn.<B>builder().header(headerB());
        }

        public final Rows row(A a, B b) {
            others.row(a);
            builder.addValues(b);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
