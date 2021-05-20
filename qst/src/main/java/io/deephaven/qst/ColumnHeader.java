package io.deephaven.qst;

import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ColumnHeader<T> {

    public static <T> ColumnHeader<T> of(String name, Class<T> clazz) {
        return of(name, ColumnType.find(clazz));
    }

    public static <T> ColumnHeader<T> of(String name, ColumnType<T> type) {
        return ImmutableColumnHeader.<T>builder().name(name).type(type).build();
    }

    public static ColumnHeader<Integer> ofInt(String name) {
        return of(name, IntType.instance());
    }

    public static ColumnHeader<String> ofString(String name) {
        return of(name, StringType.instance());
    }

    public static ColumnHeader<Double> ofDouble(String name) {
        return of(name, DoubleType.instance());
    }

    public static <A, B> ColumnHeaders2<A, B> of(ColumnHeader<A> a, ColumnHeader<B> b) {
        return a.header(b);
    }

    public static <A, B, C> ColumnHeaders3<A, B, C> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c) {
        return a.header(b).header(c);
    }

    public static <A, B, C, D> ColumnHeaders4<A, B, C, D> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c, ColumnHeader<D> d) {
        return a.header(b).header(c).header(d);
    }

    public static <A, B, C, D, E> ColumnHeaders5<A, B, C, D, E> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c, ColumnHeader<D> d, ColumnHeader<E> e) {
        return a.header(b).header(c).header(d).header(e);
    }

    public abstract String name();

    public abstract ColumnType<T> type();

    public final ColumnHeader<T> headerA() {
        return this;
    }

    public final <B> ColumnHeaders2<T, B> header(String name, Class<B> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <B> ColumnHeaders2<T, B> header(ColumnHeader<B> header) {
        return ImmutableColumnHeaders2.<T, B>builder()
          .headerA(this)
          .headerB(header)
          .build();
    }

    public final Column<T> withData(T... data) {
        return ImmutableColumn.<T>builder()
            .header(this)
            .addValues(data)
            .build();
    }

    public final Column<T> withData(Iterable<T> data) {
        return ImmutableColumn.<T>builder()
            .header(this)
            .addAllValues(data)
            .build();
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(T a) {
        return start().row(a);
    }

    public class Rows extends NewTableBuildable {

        private final ImmutableColumn.Builder<T> builder;

        Rows() {
            builder = ImmutableColumn.<T>builder().header(ColumnHeader.this);
        }

        public final Rows row(T a) {
            builder.addValues(a);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.of(builder.build());
        }
    }
}
