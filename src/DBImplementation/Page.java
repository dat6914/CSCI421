package DBImplementation;

import java.lang.reflect.Array;
import java.util.*;
import DBImplementation.Record;

public class Page {
    // Current Page Size
    private int CurrentSize;
    // Current Page ID
    private int PageID;

    private ArrayList<String> PrimaryKey = new ArrayList<>();

    // Record objects
    // primary_key, record
    private Map<String,Record> RecordObj = new HashMap<>();

    // page Constructor
    public Page(int CurrentSize, int PageID, ArrayList<String> PrimaryKey,Map<String,Record> RecordObj){
        this.CurrentSize = CurrentSize;
        this.PageID = PageID;
        this.PrimaryKey = PrimaryKey;
        this.RecordObj = RecordObj;

    }

    // gets the sum of all the records in this page
    public int getCurrentSize(){
        int sum = 0;
        for (Record records : RecordObj.values()){
            sum += records.getSize();
        }
        return sum;
    }
}
