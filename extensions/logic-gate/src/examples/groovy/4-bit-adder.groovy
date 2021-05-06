import io.deephaven.db.tables.libs.QueryLibrary
import io.deephaven.db.tables.utils.TableTools
import io.deephaven.logicgate.Helper
import io.deephaven.logicgate.LookupTableNand
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

a.s0().set() // 1
a.s1().set() // 2
a.s2().set() // 4
a.s3().set() // 8

b.s0().set() // 1
b.s1().set() // 2
b.s2().set() // 4
b.s3().set() // 8

a.s0().clear() // 1
a.s1().clear() // 2
a.s2().clear() // 4
a.s3().clear() // 8

b.s0().clear() // 1
b.s1().clear() // 2
b.s2().clear() // 4
b.s3().clear() // 8