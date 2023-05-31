/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.AsOfJoinMatch;
import io.deephaven.api.AsOfJoinRule;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see io.deephaven.api.TableOperations#aj(Object, Collection, AsOfJoinMatch, Collection)
 */
@Immutable
@NodeStyle
public abstract class AsOfJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableAsOfJoinTable.builder();
    }

    public abstract AsOfJoinMatch joinMatch();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<AsOfJoinTable, Builder> {

        Builder joinMatch(AsOfJoinMatch joinMatch);
    }
}
