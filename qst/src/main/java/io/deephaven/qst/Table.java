package io.deephaven.qst;

public interface Table {

    Table head(long size);

    Table tail(long size);
}
