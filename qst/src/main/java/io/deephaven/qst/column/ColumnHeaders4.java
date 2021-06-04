package io.deephaven.qst.column;

import io.deephaven.qst.NewTableBuildable;
import io.deephaven.qst.TableHeader;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders4<A, B, C, D> {

    @Parameter
    public abstract ColumnHeader<D> headerD();

    @Parameter
    public abstract ColumnHeaders3<A, B, C> others();

    public final <E> ColumnHeaders5<A, B, C, D, E> header(String name, Class<E> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <E> ColumnHeaders5<A, B, C, D, E> header(String name, ColumnType<E> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <E> ColumnHeaders5<A, B, C, D, E> header(ColumnHeader<E> header) {
        return ImmutableColumnHeaders5.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerD()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
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
