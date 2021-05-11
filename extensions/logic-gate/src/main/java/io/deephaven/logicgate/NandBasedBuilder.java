package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.KeyedArrayBackedMutableTable;
import java.time.Duration;
import org.immutables.value.Value.Immutable;

/**
 * Use a {@link NandBuilder} as the functionally complete set.
 *
 * @see <a href="https://en.wikipedia.org/wiki/NAND_logic">NAND logic</a>
 */
@Immutable
public abstract class NandBasedBuilder implements LogicGateBuilder {

    public abstract BitBuilder bitBuilder();

    public abstract NandBuilder nandBuilder();

    @Override
    public final Table zero() {
        return bitBuilder().zero();
    }

    @Override
    public final Table one() {
        return bitBuilder().one();
    }

    @Override
    public final SettableBit settable() {
        return bitBuilder().settable();
    }

    @Override
    public final Table timedBit(Duration duration) {
        return bitBuilder().timedBit(duration);
    }

    @Override
    public final Table nand(Table a, Table b) {
        return nandBuilder().nand(a, b);
    }

    @Override
    public final Table not(Table a) {
        return nandBuilder().nand(a, a);
    }

    @Override
    public final Table and(Table a, Table b) {
        Table nandAB = nand(a, b);
        return nand(nandAB, nandAB);
    }

    @Override
    public final Table or(Table a, Table b) {
        return nand(not(a), not(b));
    }

    @Override
    public final Table nor(Table a, Table b) {
        return not(or(a, b));
    }

    @Override
    public final Table xor(Table a, Table b) {
        Table nandAB = nand(a, b);
        return nand(nand(a, nandAB), nand(b, nandAB));
        // there is another form of this too
        // return nand(nand(b, not(a)), nand(a, not(b)));
    }

    @Override
    public final Table xnor(Table a, Table b) {
        // This is the easiest to construct
        // return not(xor(a, b));
        // But this version has better propogation delays
        return nand(nand(not(a), not(b)), nand(a, b));
    }
}
