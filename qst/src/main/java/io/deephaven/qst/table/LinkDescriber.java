package io.deephaven.qst.table;

import java.util.Iterator;
import java.util.Objects;

/**
 * Provides a potentially descriptive label for the parents of a {@link Table}.
 */
public class LinkDescriber extends TableVisitorGeneric {

    public interface LinkConsumer {
        void link(Table table);

        void link(Table table, int linkIndex);

        void link(Table table, String linkLabel);
    }

    private final LinkConsumer consumer;

    public LinkDescriber(LinkConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void accept(Table t) {
        Iterator<Table> it = ParentsVisitor.getParents(t).iterator();
        if (!it.hasNext()) {
            return;
        }
        Table first = it.next();
        if (!it.hasNext()) {
            consumer.link(first);
            return;
        }
        consumer.link(first, 0);
        for (int i = 1; it.hasNext(); ++i) {
            consumer.link(it.next(), i);
        }
    }

    public void join(Join join) {
        consumer.link(join.left(), "left");
        consumer.link(join.right(), "right");
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        join(naturalJoinTable);
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        join(exactJoinTable);
    }

    @Override
    public void visit(JoinTable joinTable) {
        join(joinTable);
    }

    @Override
    public void visit(LeftJoinTable leftJoinTable) {
        join(leftJoinTable);
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        join(aj);
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        join(raj);
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        consumer.link(whereInTable.left(), "left");
        consumer.link(whereInTable.right(), "right");
    }

    @Override
    public void visit(WhereNotInTable whereNotInTable) {
        consumer.link(whereNotInTable.left(), "left");
        consumer.link(whereNotInTable.right(), "right");
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        consumer.link(snapshotTable.base(), "base");
        consumer.link(snapshotTable.trigger(), "trigger");
    }
}
