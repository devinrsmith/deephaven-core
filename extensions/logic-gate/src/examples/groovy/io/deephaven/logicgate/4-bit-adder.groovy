package io.deephaven.logicgate

import io.deephaven.db.tables.libs.QueryLibrary
import io.deephaven.db.tables.utils.TableTools
import io.deephaven.logicgate.circuit.FullAdder1BitBuilderImpl
import io.deephaven.logicgate.circuit.SettableBits4

QueryLibrary.importStatic(Helper.class)

logic = LookupTableNand.create()
a = SettableBits4.create(logic)
b = SettableBits4.create(logic)
adder4 = FullAdder1BitBuilderImpl.create(logic).to4Bits()
out = adder4.build(a.toBits(), b.toBits(), logic.zero())

// ------------------------------------------------------------------------------------------------

A = out.aIn().merge()
B = out.bIn().merge()
CARRY = out.cOut()
C = out.s().merge()

// ------------------------------------------------------------------------------------------------

A_SUM = A.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
B_SUM = B.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
C_SUM = TableTools.merge(C.view("Q=Q*pow2(i)"), CARRY.view("Q=Q*16L")).view("Sum=Q").sumBy()

// ------------------------------------------------------------------------------------------------

a.set((byte)42)
b.set((byte)13)

// a.clear()
// b.clear()