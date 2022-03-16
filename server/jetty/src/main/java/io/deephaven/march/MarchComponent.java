package io.deephaven.march;

import io.deephaven.engine.table.Table;

import javax.inject.Named;

public interface MarchComponent {

    @Named("teams")
    Table teams();

//    @Named("rounds")
//    Table rounds();

    @Named("matches")
    Table matches();

    @Named("votes")
    Table votes();
}
