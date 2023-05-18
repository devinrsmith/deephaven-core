/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

final class SqlRootContext {

    public static SqlRootContext of(RelNode root) {
        return new SqlRootContext(root, encounterOrder(root));
    }

    private final RelNode root;
    private final Map<RelNode, Integer> repeatableId;

    private SqlRootContext(RelNode root, Map<RelNode, Integer> repeatableId) {
        this.root = Objects.requireNonNull(root);
        this.repeatableId = Objects.requireNonNull(repeatableId);
    }

    public RelNode root() {
        return root;
    }

    public NamedAdapter namedAdapter() {
        return new NamedAdapter(this);
    }

    public IndexRef createIndexRef(String prefix, RelNode node) {
        // Calcite RelNode#getId is distinct for every relational expression that is created, even if the same SQL query
        // string is parsed again. From a unit testing perspective, and a server cacheability perspective, we would
        // prefer to assign consistent (internal) column names for the same SQL query string.
        final String columnPrefix = String.format("%s%d_", prefix, repeatableId(node));
        return new IndexRef(this, columnPrefix, 0);
    }

    private int repeatableId(RelNode node) {
        final Integer id = repeatableId.get(node);
        if (id == null) {
            throw new IllegalArgumentException("node does not belong to this context");
        }
        return id;
    }

    private static Map<RelNode, Integer> encounterOrder(RelNode root) {
        // Note: the specific traversal pattern is not important, it's just important that there is some repeatable
        // pattern where we can assign a consistent id based on the rel node structures.
        int encounterOrder = 0;
        final Map<RelNode, Integer> order = new HashMap<>();
        final Queue<RelNode> toVisit = new ArrayDeque<>();
        toVisit.add(root);
        RelNode current;
        while ((current = toVisit.poll()) != null) {
            order.put(current, encounterOrder++);
            toVisit.addAll(current.getInputs());
        }
        return order;
    }
}
