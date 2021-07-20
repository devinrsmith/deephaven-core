package io.deephaven.qst.column.header;

import io.deephaven.qst.SimpleStyle;
import io.deephaven.qst.table.NewTableBuildable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.stream.Stream;

import static io.deephaven.qst.column.header.ColumnHeader.BUILDER_INITIAL_CAPACITY;

@Immutable
@SimpleStyle
public abstract class ColumnHeaders3<A, B, C> {

    @Parameter
    public abstract ColumnHeader<C> headerC();

    @Parameter
    public abstract ColumnHeaders2<A, B> others();

    public final <D> ColumnHeaders4<A, B, C, D> header(String name, Class<D> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <D> ColumnHeaders4<A, B, C, D> header(String name, Type<D> type) {
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

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders2<A, B>.Rows others;
        private final ArrayBuilder<C, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerC().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c) {
            others.row(a, b);
            builder.add(c);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<C> thisColumn = Column.of(headerC().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
