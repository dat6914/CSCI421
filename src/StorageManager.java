import java.util.ArrayList;

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    String db_loc;
    int page_size;
    int buffer_page;
    boolean databaseExist;
    public StorageManager(String db_loc, int page_size, int buffer_page, boolean databaseExist) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_page = buffer_page;
    }

    public Record getRecordByPrimaryKey(PrimaryKey primaryKey) {
        Record record = null;
        return record;
    }

    public Page getPageByTableAndPageID(Table table, int pageID) {
        Page page = null;
        return page;
    }

    public ArrayList<Record> getAllRecordsByTableID(int tableID) {
        ArrayList<Record> recordsArrayList = new ArrayList<>();
        return recordsArrayList;
    }

    public void insertRecordToATable(Table table) {

    }

    public void deleteRecordByPrimaryKeyForATable(PrimaryKey primaryKey, Table table) {

    }

    public void updateRecordByPrimaryKeyInATable(PrimaryKey primaryKey, Table table) {

    }


}
