package DBImplementation;

import java.awt.*;
import java.lang.reflect.Array;
import java.util.*;
import DBImplementation.Record;

public class Page {
    // Current Page Size
    private int CurrentSize;
    // Current Page ID
    private int PageID;

    private ArrayList<Pointer> pointerList = new ArrayList<>();
    // Record objects
    private ArrayList<Record> recordList = new ArrayList<>();

    // page Constructor
    public Page(int CurrentSize, int PageID){
        this.CurrentSize = CurrentSize;
        this.PageID = PageID;
    }

    // gets the sum of all the records in this page
    public void updateCurrentSize(){
        int sum = 0;
        for (Record records : recordList){
            sum += records.getSize();
        }
        CurrentSize = sum;
    }

    public void insertPointerToPointerList(Pointer pointer){
        pointerList.add(pointer);
    }




    public ArrayList<Record> getRecordList(){
        return recordList;
    }

    public void updatePageID(){
        PageID += 1;
    }

    public int getPageID(){
        return PageID;
    }

}
