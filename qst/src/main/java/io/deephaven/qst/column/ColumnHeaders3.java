package io.deephaven.qst.column;

import io.deephaven.qst.NewTableBuildable;
import io.deephaven.qst.TableHeader;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders3<A, B, C> {

    @Parameter
    public abstract ColumnHeader<C> headerC();

    @Parameter
    public abstract ColumnHeaders2<A, B> others();

    public final <D> ColumnHeaders4<A, B, C, D> header(String name, Class<D> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <D> ColumnHeaders4<A, B, C, D> header(String name, ColumnType<D> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <D> ColumnHeaders4<A, B, C, D> header(ColumnHeader<D> header) {
        return ImmutableColumnHeaders4.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerC()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
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
