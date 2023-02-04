import java.util.ArrayList;

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    private String db_loc;
    private int page_size;
    private int buffer_size;
    private boolean databaseExist;
    public StorageManager(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
    }

    public Record getRecordByPrimaryKey(String primaryKeyName) {
        Record record = null;
        return record;
    }

    public Page getPageByTableAndPageID(String table, int pageID) {
        Page page = null;
        return page;
    }

    public ArrayList<Record> getAllRecordsByTableID(int tableID) {
        ArrayList<Record> recordsArrayList = new ArrayList<>();
        return recordsArrayList;
    }

    public boolean insertRecordToATable(String tableID, Record record) {
        return false;
    }

    public boolean deleteRecordByPrimaryKeyForATable(String primaryKeyName, String tableID) {
        return false;
    }

    public boolean updateRecordByPrimaryKeyInATable(String primaryKeyName, Table tableID, Record record) {
        return false;
    }


}
