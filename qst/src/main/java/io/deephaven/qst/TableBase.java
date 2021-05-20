package io.deephaven.qst;

public abstract class TableBase implements Table {

    @Override
    public final HeadTable head(long size) {
        return ImmutableHeadTable.of(this, size);
    }

    @Override
    public final TailTable tail(long size) {
        return ImmutableTailTable.of(this, size);
    }
}
