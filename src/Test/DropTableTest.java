package Test;

import Main.Catalog;
import Main.PageBuffer;
import Main.Table;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

public class DropTableTest {

    @Test
    public void test() throws IOException {
        //create table
        Catalog catalog = new Catalog("");

        //drop table
        PageBuffer pageBuffer = new PageBuffer("", 4, 4096);
        Table table = new Table("table1", "id", new ArrayList<String>(), new ArrayList<String>(), "", new ArrayList<Integer>());
        pageBuffer.getStorageManager().getCatalog().getTablesList().add(table);
        pageBuffer.dropTable("table1");
        //pageBuffer.quitProgram(pageBuffer.getStorageManager(), pageBuffer.getPagelistBuffer()); //uncomment as needed when a catalog.txt exisits in the db_loc
    }
}
