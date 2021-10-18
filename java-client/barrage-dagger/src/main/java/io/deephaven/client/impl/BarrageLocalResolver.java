package io.deephaven.client.impl;

import io.deephaven.db.tables.Table;

public interface BarrageLocalResolver {

    Table resolveQueryScopeName(String queryScopeName);

    Table resolveApplicationField(String applicationId, String fieldName);
}
