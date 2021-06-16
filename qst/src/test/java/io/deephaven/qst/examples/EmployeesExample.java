package io.deephaven.qst.examples;

import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.column.header.ColumnHeader;

public class EmployeesExample {

    public static NewTable employees() {
        return ColumnHeader
            .of(ColumnHeader.ofString("LastName"), ColumnHeader.ofInt("DeptId"),
                ColumnHeader.ofString("Telephone"))
            .row("Rafferty", 31, "(347) 555-0123").row("Jones", 33, "(917) 555-0198")
            .row("Steiner", 33, "(212) 555-0167").row("Robins", 34, "(952) 555-0110")
            .row("Smith", 34, null).row("Rogers", null, null).build();
    }

    public static NewTable departments() {
        return ColumnHeader
            .of(ColumnHeader.ofInt("DeptId"), ColumnHeader.ofString("DeptName"),
                ColumnHeader.ofString("Telephone"))
            .row(31, "Sales", "(646) 555-0134").row(33, "Engineering", "(646) 555-0178")
            .row(34, "Clerical", "(646) 555-0159").row(35, "Marketing", "(212) 555-0111").build();
    }

    public static JoinTable joined() {
        return employees().join(departments(), "DeptId", "DeptName,DeptTelephone=Telephone");
    }
}
