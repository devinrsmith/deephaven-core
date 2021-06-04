package io.deephaven.qst.column;

import io.deephaven.qst.NewTableBuildable;
import io.deephaven.qst.TableHeader;
import io.deephaven.qst.column.type.ColumnType;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders7<A, B, C, D, E, F, G> {

    @Parameter
    public abstract ColumnHeader<G> headerG();

    @Parameter
    public abstract ColumnHeaders6<A, B, C, D, E, F> others();

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(String name, Class<H> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(String name, ColumnType<H> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(ColumnHeader<H> header) {
        return ImmutableColumnHeaders8.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerG()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e, F f, G g) {
        return start().row(a, b, c, d, e, f, g);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders6<A, B, C, D, E, F>.Rows others;
        private final ImmutableColumn.Builder<G> builder;

        Rows() {
            others = others().start();
            builder = ImmutableColumn.<G>builder().header(headerG());
        }

        public final Rows row(A a, B b, C c, D d, E e, F f, G g) {
            others.row(a, b, c, d, e, f);
            builder.addValues(g);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
