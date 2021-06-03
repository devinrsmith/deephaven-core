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

    public static ColumnHeader<Boolean> ofBoolean(String name) {
        return of(name, ColumnType.booleanType());
    }

    public static ColumnHeader<Integer> ofInt(String name) {
        return of(name, ColumnType.intType());
    }

    public static ColumnHeader<Long> ofLong(String name) {
        return of(name, ColumnType.longType());
    }

    public static ColumnHeader<String> ofString(String name) {
        return of(name, ColumnType.stringType());
    }

    public static ColumnHeader<Double> ofDouble(String name) {
        return of(name, ColumnType.doubleType());
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

    public final Column<T> emptyData() {
        return Column.empty(this);
    }

    public final Column<T> withData(T... data) {
        return Column.of(this, data);
    }

    public final Column<T> withData(Iterable<T> data) {
        return Column.of(this, data);
    }

    public final <R> Column<T> withData(TypeLogic typeLogic, ColumnType<R> fromType, Iterable<R> fromData) {
        final ColumnBuilder<T> builder = withBuilder();
        for (R fromDatum : fromData) {
            builder.add(typeLogic.transform(type(), fromType, fromDatum));
        }
        return builder.build();
    }

    public final ColumnBuilder<T> withBuilder() {
        return Column.builder(this);
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(headerA());
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
