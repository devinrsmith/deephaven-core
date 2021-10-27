package io.deephaven.qst.table;

import io.deephaven.annotations.LeafStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;

@Immutable
@LeafStyle
public abstract class UriTable extends TableBase {

    // TODO, provide invalidation ?
    // private static final UUID ZERO_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    public static UriTable of(URI uri) {
        return ImmutableUriTable.of(uri);
    }

    @Parameter
    public abstract URI uri();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
