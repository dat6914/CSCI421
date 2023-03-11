package Main;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class TableFile {
    private Table table;
    private ArrayList<Page> pageList;
    private int pageSize;
    private String db_loc;

    public TableFile(Table table, int pageSize, String db_loc) {
        this.table = table;
        this.pageSize = pageSize;
        this.db_loc = db_loc;
        this.pageList = new ArrayList<>();
    }

    public ArrayList<Page> getPageList() {
        return this.pageList;
    }

}
