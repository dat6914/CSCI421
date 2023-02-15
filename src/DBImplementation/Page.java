package DBImplementation;

import java.util.*;


public class Page {
    // Current Page ID
    private int PageID;
    private ArrayList<Pointer> pointerList = new ArrayList<>();
    // DBImplementation.Record objects
    private ArrayList<Record> recordList = new ArrayList<>();

    // byte array of records
    private byte recordByte[] = new byte[4096];

    // number of record
    private int numOfRecord;

    // page Constructor
    public Page(int PageID){
        this.PageID = PageID;
        this.numOfRecord = numOfRecord;
        this.pointerList = pointerList;
        this.recordByte = recordByte;
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

    public boolean recordExist(String primaryKey){
        int index = 0;
        for (Record record : recordList){
            if(record.getPrimaryKey().equals(primaryKey)){
                return true;
            }
        }
        return false;
    }


    public void insertRec(Pointer pointer, byte[] record){
        for (int i = this.recordByte.length; i > 0; i--) {


        }
    }

}
