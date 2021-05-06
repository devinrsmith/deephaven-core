logic = io.deephaven.logicgate.LookupTableNand.create()

a0 = logic.settable()
a1 = logic.settable()
a2 = logic.settable()
a3 = logic.settable()

b0 = logic.settable()
b1 = logic.settable()
b2 = logic.settable()
b3 = logic.settable()

a = io.deephaven.logicgate.circuit.ImmutableBits4.builder().b0(a0.bit()).b1(a1.bit()).b2(a2.bit()).b3(a3.bit()).build()
b = io.deephaven.logicgate.circuit.ImmutableBits4.builder().b0(b0.bit()).b1(b1.bit()).b2(b2.bit()).b3(b3.bit()).build()

aComponents = a.mergeForDisplay()
//aSum = aComponents.sumBy()

bComponents = b.mergeForDisplay()
//bSum = bComponents.sumBy()

adder1 = io.deephaven.logicgate.circuit.ImmutableFullAdder1BitImpl.builder().xor(logic).and(logic).or(logic).build()
adder4 = io.deephaven.logicgate.circuit.ImmutableFullAdder4BitImpl.builder().adder(adder1).build()
aPlusB = adder4.build(a, b, logic.zero())
aPlusBComponents = aPlusB.s().mergeForDisplay()
//aPlusBSum = aPlusBComponents.sum()
carryBit = aPlusB.cOut()

a0.set()
a1.set()
a2.set()
a3.set()

b0.set()
b1.set()
b2.set()
b3.set()

a0.clear()
a1.clear()
a2.clear()
a3.clear()

b0.clear()
b1.clear()
b2.clear()
b3.clear()
