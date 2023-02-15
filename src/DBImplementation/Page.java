package DBImplementation;

import java.util.*;

public class Page {
    // Current Page Size
    private int CurrentSize;
    // Current Page ID
    private int PageID;

    private ArrayList<Pointer> pointerList = new ArrayList<>();
    // DBImplementation.Record objects
    private ArrayList<Record> recordList = new ArrayList<>();

    // page Constructor
    public Page(int PageID){
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

    // gets the sum of all the records in this page
    public int getCurrentSize(){
        return CurrentSize;
    }

    public boolean recordExist(String primaryKey){
        int index = 0;
        for (Record record : recordList){
            if(record.getPrimaryKey().equals(primaryKey)){
                return true;
            }
        }
        return false;
    }

    public boolean PageFull(Record CurrRecSize){
        int PageMax = 4096;
        int PageSize = getCurrentSize();
        return (CurrRecSize.getSize() + PageSize) >= PageMax;
    }


}
