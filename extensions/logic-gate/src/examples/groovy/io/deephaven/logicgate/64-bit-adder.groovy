package io.deephaven.logicgate

import io.deephaven.db.tables.libs.QueryLibrary
import io.deephaven.logicgate.Helper
import io.deephaven.logicgate.LookupTableNand
import io.deephaven.logicgate.circuit.FullAdder1BitBuilderImpl
import io.deephaven.logicgate.circuit.SettableBits64

QueryLibrary.importStatic(Helper.class)

logic = LookupTableNand.create()
a = SettableBits64.create(logic)
b = SettableBits64.create(logic)
adder64 = FullAdder1BitBuilderImpl.create(logic).to4Bits().to16Bits().to64Bits()
out = adder64.build(a.toBits(), b.toBits(), logic.zero())

// ------------------------------------------------------------------------------------------------

A = out.aIn().merge()
B = out.bIn().merge()
CARRY = out.cOut()
C = out.s().merge()

// ------------------------------------------------------------------------------------------------

A_SUM = A.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
B_SUM = B.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
C_SUM = C.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()

// ------------------------------------------------------------------------------------------------

a.set(32768L)
b.set(42424242L)

// a.clear()
// b.clear()