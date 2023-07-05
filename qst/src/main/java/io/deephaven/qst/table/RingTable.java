package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class RingTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableRingTable.builder();
    }

    public static RingTable of(TableSpec parent, int size) {
        return builder()
                .parent(parent)
                .size(size)
                .build();
    }

    @Override
    public abstract TableSpec parent();

    public abstract int size();

    @Default
    public boolean initialize() {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException(
                    String.format("ring table must have a non-negative size: %d", size()));
        }
    }

    public interface Builder {
        Builder parent(TableSpec parent);

        Builder size(int size);

        Builder initialize(boolean initialize);

        RingTable build();
    }
}
