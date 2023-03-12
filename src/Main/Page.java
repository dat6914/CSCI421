package Main;

import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This class is to store the list of record and the table that the page belongs to
 * and the actual size of this page
 */
public class Page {
    private int pageID;
    private ArrayList<Record> record_list = new ArrayList<>();
    private ArrayList<Object> primarykeyValueList = new ArrayList<>();
    private int current_page_size;
    private ArrayList<Pointer> pointerList;
    private String DBLocation;
    private String tablename;

    public Page(int pageID) {
        this.pageID = pageID;
    }

    public Page(int pageID, String tableName, String DBLocation ) {
        this.pageID = pageID;
        this.DBLocation = DBLocation;
        this.tablename = tableName;
        //this.record_list = getRecordListFromPage(pageID);
        this.current_page_size = computeCurrentPagesize(record_list);
    }

    // constructor for when creating a new page
    // not pulling from hardware
    public Page(int pageID, ArrayList<Pointer> pointers,ArrayList<Record> record_list){
        this.pageID = pageID;
        this.pointerList = pointers;
        this.record_list = record_list;

    }

    public Page(int pageID, String tablename, ArrayList<Record> record_list) {
        this.pageID = pageID;
        this.tablename = tablename;
        this.record_list = record_list;
    }

    public String getTablename(){
        return this.tablename;
    }


    /**
     * Method gets the current size of page
     * @return the current page size
     */
    public int getCurrent_page_size() {
        return this.current_page_size;
    }


    /**
     * Method compute the current page size
     * @param record_list arraylist of records
     * @return the current page size
     */
    public int computeCurrentPagesize(ArrayList<Record> record_list) {
        int size = 0;
        size = size + Integer.BYTES;
        int pointerSize = Integer.BYTES * 2;
        for (Record record : record_list) {
            size = size + pointerSize + record.getRecordSize();
        }
        return size;
    }

    /**
     * Method increases the current page size when insert a record
     * @param record record is inserted
     * @param recordByteArr byte[] of record
     */
    public void incCurrentPageSize(Record record, byte[] recordByteArr){
        this.current_page_size = this.current_page_size + record.getValuesList().size() + 8 + recordByteArr.length;
    }

    /**
     * Method gets page ID
     * @return page ID
     */
    public int getPageID() {
        return this.pageID;
    }

    /**
     * Method decreases the current page size when insert a record
     * @param record record is removed
     * @param recordByteArr byte[] of record
     */
    public void decCurrentPageSize(Record record, byte[] recordByteArr) {
        this.current_page_size = this.current_page_size - record.getValuesList().size() - 8 - recordByteArr.length;
    }

    /**
     * Method gets the list of records
     * @return arraylist of records
     */
    public ArrayList<Record> getRecordList() {
        return this.record_list;
    }

    public ArrayList<Pointer> getPointerList(){
        return this.pointerList;
    }

    /**
     * Methods gets the list of records from a particular page from a given table
     * @param pageID page ID
     * @return Arraylist of record
     */
//    public ArrayList<Record> getRecordListFromPage(int pageID,){
//
//    }

    /**
     * Method read the page file
     * @param path path of page file
     * @return byte array of page file
     */
    public static byte[] readPageFile(String path) {
        try {
            File file = new File(path);
            FileInputStream fs = new FileInputStream(file);
            byte[] arr = new byte[(int) file.length()];
            fs.read(arr);
            fs.close();
            return arr;
        } catch (IOException e) {
            System.err.println("An error occurred while reading the file.");
            e.printStackTrace();
            System.err.println("ERROR");
            return null;
        }
    }

    public int getNumRec(){
        return this.record_list.size();
    }


    /**
     * Method converts Main.Page to byte array
     * @param page
     * @return byte array of page
     */
    public byte[] convertPageToByteArr(Page page, int page_size) {
        int indextracking = 0;
        byte[] result = new byte[page_size];
        byte[] pageId = ByteBuffer.allocate(Integer.BYTES).putInt(page.getPageID()).array();
        System.arraycopy(pageId,0,result,indextracking,pageId.length);
        indextracking += pageId.length;

        byte[] numRecordArr = ByteBuffer.allocate(Integer.BYTES).putInt(page.getNumRec()).array();
        System.arraycopy(numRecordArr, 0, result, indextracking, numRecordArr.length);
        indextracking += numRecordArr.length;
        ArrayList<Record> record_list = page.getRecordList();

        int indexReverse = page_size;
        for (int i = 0; i < record_list.size(); i++) {
            Record record = record_list.get(i);
            byte[] recordArr = record.convertRecordToByteArr(record);
            int length = recordArr.length;
            indexReverse = indexReverse - length;
            byte[] offSetArr = ByteBuffer.allocate(Integer.BYTES).putInt(indexReverse).array();
            System.arraycopy(offSetArr, 0, result, indextracking, offSetArr.length);
            indextracking = indextracking + offSetArr.length;

            byte[] lengthArr = ByteBuffer.allocate(Integer.BYTES).putInt(length).array();
            System.arraycopy(lengthArr, 0, result, indextracking, lengthArr.length);
            indextracking = indextracking + lengthArr.length;

            System.arraycopy(recordArr, 0, result, indexReverse , recordArr.length);

        }

        return result;
    }


    /**
     * Method converts byte array of page to arraylist of records
     * @param byteArr byte array of page
     * @return arraylist of records
     */
    public Page convertByteArrToPage(byte[] byteArr, int pageID, String tablename) {
        ArrayList<Record> recordArrayList = new ArrayList<>();
        int indextracking = 0;
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArr);
        int pageId = byteBuffer.getInt();
        indextracking += Integer.BYTES;
        int recordNum = byteBuffer.getInt();
        indextracking += Integer.BYTES;

        for (int i = 0; i < recordNum; i++) {
            int offset = byteBuffer.getInt(indextracking);
            indextracking = indextracking + Integer.BYTES;
            int length = byteBuffer.getInt(indextracking);
            indextracking = indextracking + Integer.BYTES;

            byte[] recordArr = Arrays.copyOfRange(byteArr, offset, offset+length);
            Record record = Record.convertByteArrToRecord(recordArr);
            recordArrayList.add(record);

        }

        Page page = new Page(pageID, tablename, recordArrayList);

        return page;
    }


    /**
     * This method appends the byte array to the current ByteBuffer
     * @param current the current ByteBuffer
     * @param arr   the array needs to be appended
     * @return ByteBuffer after append the byte array
     */
    public static ByteBuffer appendByteBuffer(ByteBuffer current, byte[] arr) {
        ByteBuffer result = ByteBuffer.allocate(current.capacity() + arr.length);
        result.put(current.array(), 0, current.array().length);
        result.put(arr, 0, arr.length);
        return result;
    }


    @Override
    public boolean equals(Object o){
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        Page other = (Page)o;
        return pointerList.equals(other.pointerList) && record_list.equals(other.record_list);
    }

    public void setRecordList(ArrayList<Record> records) {
        this.record_list = records;
    }
}
