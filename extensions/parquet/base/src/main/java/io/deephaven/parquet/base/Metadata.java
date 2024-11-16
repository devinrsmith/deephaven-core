//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class Metadata {
    private final ParquetMetadata metadata;
    // private final boolean failOnOutOfOrderColumnChunks;

    public Metadata(ParquetMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata);
    }

    // schema

    // TODO: verify up-front

    public void verify() {
        final List<Column> columns = columns().collect(Collectors.toList());
        final List<RowGroup> rowGroups = rowGroups().collect(Collectors.toList());
        for (final RowGroup rowGroup : rowGroups) {
            for (final Column column : columns) {
                RowGroup.ColumnChunk columnChunk = rowGroup.columnChunk(column);
            }
        }
    }

    public Column column(int index) {
        return new Column(index);
    }

    public Column column(String... path) {
        return get(columns(), column -> column.hasPath(path));
    }

    public Stream<Column> columns() {
        // getPaths() a little more efficient than getColumns()
        final int numColumns = numColumns();
        return IntStream.range(0, numColumns).mapToObj(this::column);
    }

    public int numColumns() {
        return metadata.getFileMetaData().getSchema().getPaths().size();
    }

    public int numRowGroups() {
        return metadata.getBlocks().size();
    }

    public Stream<RowGroup> rowGroups() {
        return IntStream.range(0, numRowGroups()).mapToObj(this::rowGroup);
    }

    public RowGroup rowGroup(int index) {
        return new RowGroup(index);
    }

    public final class Column {
        private final int columnIndex;
        private final ColumnDescriptor columnDescriptor;

        private Column(int columnIndex) {
            this.columnIndex = columnIndex;
            this.columnDescriptor = metadata.getFileMetaData().getSchema().getColumns().get(columnIndex);
        }

        public Metadata metadata() {
            return Metadata.this;
        }

        public int columnIndex() {
            return columnIndex;
        }

        public ColumnDescriptor columnDescriptor() {
            return columnDescriptor;
        }

        private boolean hasPath(String... path) {
            return Arrays.equals(columnDescriptor.getPath(), path);
        }
    }

    public final class RowGroup {
        private final int rowGroupIndex;
        private final BlockMetaData rowGroupMetadata;

        private RowGroup(int rowGroupIndex) {
            this.rowGroupIndex = rowGroupIndex;
            this.rowGroupMetadata = metadata.getBlocks().get(rowGroupIndex);
            if (rowGroupMetadata.getOrdinal() != rowGroupIndex) {
                throw new IllegalStateException();
            }
        }

        private Metadata metadata() {
            return Metadata.this;
        }

        public int rowGroupIndex() {
            return rowGroupIndex;
        }

        public BlockMetaData rowGroupMetadata() {
            return rowGroupMetadata;
        }

        public ColumnChunk columnChunk(Column column) {
            // struct RowGroup {
            // /** Metadata for each column chunk in this row group.
            // * This list must have the same order as the SchemaElement list in FileMetaData.
            // **/
            // 1: required list<ColumnChunk> columns
            final ColumnChunkMetaData ccmd;
            final boolean isOutOfOrder;
            final ColumnChunkMetaData atIndex = column.columnIndex >= rowGroupMetadata.getColumns().size()
                    ? null
                    : rowGroupMetadata.getColumns().get(column.columnIndex);
            if (atIndex != null && hasSamePath(column, atIndex)) {
                ccmd = atIndex;
                isOutOfOrder = false;
            } else {
                ccmd = get(rowGroupMetadata.getColumns().stream(), x -> hasSamePath(column, x));
                isOutOfOrder = true;
            }
            return new ColumnChunk(column, ccmd, isOutOfOrder);
        }

        public final class ColumnChunk {
            private final Column column;
            private final ColumnChunkMetaData columnChunkMetaData;
            private final boolean isOutOfOrder;

            private ColumnChunk(Column column, ColumnChunkMetaData columnChunkMetaData, boolean isOutOfOrder) {
                this.column = Objects.requireNonNull(column);
                this.columnChunkMetaData = Objects.requireNonNull(columnChunkMetaData);
                this.isOutOfOrder = isOutOfOrder;
                if (rowGroupMetadata.getRowCount() != columnChunkMetaData.getValueCount()) {
                    throw new IllegalStateException("Invalid parquet file or reader");
                }
                if (column.columnDescriptor.getPrimitiveType() != columnChunkMetaData.getPrimitiveType()) {
                    throw new IllegalStateException("Invalid parquet file or reader");
                }
            }

            public Metadata metadata() {
                return Metadata.this;
            }

            public RowGroup rowGroup() {
                return RowGroup.this;
            }

            public Column column() {
                return column;
            }

            public ColumnChunkMetaData columnChunkMetaData() {
                return columnChunkMetaData;
            }
        }
    }

    private static boolean hasSamePath(Column column, ColumnChunkMetaData ccmd) {
        return column.hasPath(ccmd.getPath().toArray());
    }

    private static <T> T get(Stream<T> stream) {
        final List<T> out = stream.limit(2).collect(Collectors.toList());
        if (out.isEmpty()) {
            throw new IllegalStateException("TODO");
        }
        if (out.size() > 1) {
            throw new IllegalStateException("TODO");
        }
        return out.get(0);
    }

    private static <T> T get(Stream<T> stream, Predicate<T> predicate) {
        return get(stream.filter(predicate));
    }
}
