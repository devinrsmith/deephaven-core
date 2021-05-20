package io.deephaven.qst;

public class NewTableExamples {

    static NewTable what() {
        return NewTable.header("A", String.class)
            .row("A")
            .row("B")
            .row("C")
            .build();
    }

    static ColumnHeaders3<Byte, Byte, Byte> truthTableHeader() {
        return NewTable.header("A", byte.class)
            .header("B", byte.class)
            .header("Q", byte.class);
    }

    static NewTable nandLogic() {
        return truthTableHeader()
            .row((byte)0, (byte)0, (byte)1)
            .row((byte)0, (byte)1, (byte)1)
            .row((byte)1, (byte)0, (byte)1)
            .row((byte)1, (byte)1, (byte)0)
            .build();
    }

    static NewTable leftRowOriented() {
        return employeeHeader()
            .row("Rafferty", 31, "(347) 555-0123")
            .row("Jones", 33, "(917) 555-0198")
            .row("Steiner", 33, "(212) 555-0167")
            .row("Robins", 34, "(952) 555-0110")
            .row("Smith", 34, null)
            .row("Rogers", null, null)
            .row("what", 34, "test")
            .build();
    }

    static ColumnHeader<String> lastName() {
        return ColumnHeader.of("LastName", String.class);
    }

    static ColumnHeader<Integer> departmentId() {
        return ColumnHeader.of("DeptID", int.class);
    }

    static ColumnHeader<String> telephone() {
        return ColumnHeader.of("Telephone", String.class);
    }

    static NewTable leftRowOriented2() {
        return employeeHeader()
            .row("Rafferty", 31, "(347) 555-0123")
            .build();
    }

    private static ColumnHeaders3<String, Integer, String> employeeHeader() {
        return ColumnHeader.of(
          lastName(),
          departmentId(),
          telephone());
    }

    static NewTable leftColumnOriented() {
        return NewTable.of(
            Column.of("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"),
            Column.of("DeptId", 31, 33, 33, 34, 34, null),
            Column.of("Telephone", "(347) 555-0123", "(917) 555-0198", "(212) 555-0167", "(952) 555-0110", null, null));
    }

    static ColumnHeaders3<Integer, String, String> deptInfoHeader() {
        return NewTable.header("DeptID", int.class)
            .header("DeptName", String.class)
            .header("Telephone", String.class);
    }

    static NewTable right() {
        return deptInfoHeader()
            .row(31, "Sales", "(646) 555-0134")
            .row(33, "Engineering", "(646) 555-0178")
            .row(34, "Clerical", "(646) 555-0159")
            .row(35, "Marketing", "(212) 555-0111")
            .build();
    }

    static void test() {


    }

}
