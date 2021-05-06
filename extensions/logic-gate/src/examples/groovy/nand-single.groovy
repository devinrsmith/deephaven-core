import io.deephaven.logicgate.LookupTableNand

logic = LookupTableNand.create()
a = logic.settable()
b = logic.settable()

A = a.bit()
B = b.bit()
OUT = logic.nand(A, B)

a.set()
b.set()

a.clear()
b.clear()