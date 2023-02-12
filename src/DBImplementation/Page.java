package DBImplementation;

import java.lang.reflect.Array;
import java.util.*;
import DBImplementation.Record;

public class Page {
    private int CurrentSize;

    private int PageID;
    private Record records;

    private ArrayList<String> Record_PrimaryKey = new ArrayList<>();

    private  Map<String,Record> RecordObj = new HashMap<>();

}
