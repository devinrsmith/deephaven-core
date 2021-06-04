package io.deephaven.qst.column;

import io.deephaven.qst.NewTableBuildable;
import io.deephaven.qst.TableHeader;
import io.deephaven.qst.column.type.ColumnType;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders8<A, B, C, D, E, F, G, H> {

    @Parameter
    public abstract ColumnHeader<H> headerH();

    @Parameter
    public abstract ColumnHeaders7<A, B, C, D, E, F, G> others();

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(String name, Class<I> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(String name, ColumnType<I> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(ColumnHeader<I> header) {
        return ImmutableColumnHeaders9.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerH()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h) {
        return start().row(a, b, c, d, e, f, g, h);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders7<A, B, C, D, E, F, G>.Rows others;
        private final ImmutableColumn.Builder<H> builder;

        Rows() {
            others = others().start();
            builder = ImmutableColumn.<H>builder().header(headerH());
        }

        public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h) {
            others.row(a, b, c, d, e, f, g);
            builder.addValues(h);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
