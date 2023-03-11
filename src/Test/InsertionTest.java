package Test;

import Main.Page;
import Main.Pointer;
import Main.Record;
import Main.PageBuffer;
import Main.Table;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertionTest {
    public static void main(String[] args) {
        String db_loc = "/Users/daotran/Downloads/Database";
        int page_size = 200;
        int buffer_size = 2;

        PageBuffer pageBuffer = new PageBuffer(db_loc, page_size, buffer_size);

        String createSQL = "create table foo ( num integer primarykey, name varchar(10),\n" +
                    "address char(5), inCampus boolean,\n" +
                    "gpa double);";

        Table table = pageBuffer.getStorageManager().createTable(createSQL);

        String insertSQL = "insert into foo values (1 \"foo bar\" \"123\" true 2.1)," +
                "(3\"baz\" \"ddd\" true 4.14)," +
                "(2\"bar\" \"bbbbbbb\" false 5.2)," +
                "(5 \"true\" \"aaaa\" true null);";

        ArrayList<String[]> insertSQLAfterSplit = pageBuffer.splitInsertCommandInput(insertSQL, table);
        ArrayList<Record> recordInsertList = new ArrayList<>();
        for (String[] value : insertSQLAfterSplit) {
            Record record = pageBuffer.validateDataType(value, table.getAttriType_list());
            if (record == null) {
                return;
            }
            else {
                recordInsertList.add(record);
            }
        }





        System.out.println("STOP");

    }

}


