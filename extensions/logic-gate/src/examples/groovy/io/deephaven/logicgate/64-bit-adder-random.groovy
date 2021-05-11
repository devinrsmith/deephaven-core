package io.deephaven.logicgate

import io.deephaven.db.tables.libs.QueryLibrary
import io.deephaven.logicgate.Helper
import io.deephaven.logicgate.LookupTableNand
import io.deephaven.logicgate.circuit.Bits64
import io.deephaven.logicgate.circuit.FullAdder1BitBuilderImpl

import java.time.Duration
import java.util.concurrent.ThreadLocalRandom

QueryLibrary.importStatic(Helper.class)

logic = LookupTableNand.create()
r = ThreadLocalRandom.current()
min = Duration.ofSeconds(1)
max = Duration.ofMinutes(10)
a = Bits64.ofTimed(r, min, max, logic)
b = Bits64.ofTimed(r, min, max, logic)
adder64 = FullAdder1BitBuilderImpl.create(logic).to4Bits().to16Bits().to64Bits()
out = adder64.build(a, b, logic.zero())

// ------------------------------------------------------------------------------------------------

A = out.aIn().merge()
B = out.bIn().merge()
CARRY = out.cOut()
C = out.s().merge()

// ------------------------------------------------------------------------------------------------

A_SUM = A.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
B_SUM = B.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
C_SUM = C.view("Q=Q*pow2(i)").view("Sum=Q").sumBy()
