package DBImplementation;

import java.util.ArrayList;

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    private final PageBuffer pageBuffer;
    private String db_loc;
    private int page_size;
    private int buffer_size;
    private boolean databaseExist;
    public StorageManager(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
        this.pageBuffer = new PageBuffer(buffer_size);
    }

    public void findLeastRecentlyUsedPage() {
        //TODO
        //check least recently used page and see if its been modified (compare metadata time block with File.lastModified())

    }

    public Record getRecordByPrimaryKey(String primaryKeyName) {
        Record record = null;
        return record;
    }



    public ArrayList<Record> getAllRecordsByTableID(int tableID) {
        ArrayList<Record> recordsArrayList = new ArrayList<>();
        return recordsArrayList;
    }

    public boolean insertRecordIntoTable(String tableID, Record record) {
        return false;
    }

    public boolean deleteRecordByPrimaryKeyForATable(String primaryKeyName, String tableID) {
        return false;
    }

    public boolean updateRecordByPrimaryKeyInATable(String primaryKeyName, Table tableID, Record record) {
        return false;
    }


}
