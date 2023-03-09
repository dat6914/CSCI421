package Main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    private Table table;
    private int current_page_size;
    private String DBLocation;
    private ArrayList<Pointer> pointerList;

    public Page(int pageID, Table table, String DBLocation) {
        this.pageID = pageID;
        this.table = table;
        this.DBLocation = DBLocation;
        this.record_list = getRecordListFromPage(pageID, table);
        this.current_page_size = computeCurrentPagesize(record_list);
    }


    /**
     * Method gets table of the page
     * @return table of page
     */
    public Table getTable() {
        return this.table;
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
     * @param table table
     * @return Arraylist of record
     */
    public ArrayList<Record> getRecordListFromPage(int pageID, Table table){
        String pagePath = this.DBLocation + "/Table/" + table.getTableName() + ".txt";
        File file = new File(pagePath);
        if (file.exists()) {
            //todo: do deserialize on table -> deserialize page -> find record list using page ID
            //todo: change return
            byte[] pageArr = readPageFile(pagePath);
            return convertByteArrToPage(pageArr, table);
        } else {
            return new ArrayList<>();
        }
    }

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


    /**
     * Method checks if two pages are equals
     * @param obj other page
     * @return true if equals, otherwise false
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Page)
            return ((Page) obj).pageID == pageID;
        return false;
    }

    /**
     * Method converts Main.Page to byte array
     * @param page
     * @param table table that the page belongs to
     * @param page_size the page size
     * @return byte array of page
     */
    public byte[] convertPageToByteArr(Page page, Table table, int page_size) {
        ArrayList<Record> record_list = page.getRecordList();
        // ArraayList<Main.Pointer> pointer_list = page.getPointer
        int indextracking = 0;
        int numRecord = record_list.size();
        byte[] result = new byte[page_size];
        byte[] numRecordArr = ByteBuffer.allocate(Integer.BYTES).putInt(numRecord).array();
        System.arraycopy(numRecordArr, 0, result, indextracking, numRecordArr.length);

        // result = [INDEX]
        indextracking = indextracking + numRecordArr.length;
        int indexReverse = page_size;
        for (int i = 0; i < numRecord; i++) {
            Record record = record_list.get(i);

            byte[] recordArr = record.convertRecordToByteArr(record);
            int length = recordArr.length;
            indexReverse = indexReverse - length;
            byte[] offSetArr = ByteBuffer.allocate(4).putInt(indexReverse).array();
            System.arraycopy(offSetArr, 0, result, indextracking, offSetArr.length);
            indextracking = indextracking + 4;

            byte[] lengthArr = ByteBuffer.allocate(4).putInt(length).array();
            System.arraycopy(lengthArr, 0, result, indextracking, lengthArr.length);
            indextracking = indextracking + 4;

            System.arraycopy(recordArr, 0, result, indexReverse , recordArr.length);
        }
        return result;
    }




    /**
     * Method converts byte array of page to arraylist of records
     * @param byteArr byte array of page
     * @param table table that page belongs to
     * @return arraylist of records
     */
    public ArrayList<Record> convertByteArrToPage(byte[] byteArr, Table table) {
        ArrayList<Record> recordArrayList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArr);
        int recordNum = byteBuffer.getInt(0);
        int indextracking = 4;

        for (int i = 0; i < recordNum; i++) {
            int offset = byteBuffer.getInt(indextracking);
            indextracking = indextracking + 4;
            int length = byteBuffer.getInt(indextracking);
            indextracking = indextracking + 4;

            byte[] recordArr = Arrays.copyOfRange(byteArr, offset, offset+length);
            Record record = Record.convertByteArrToRecord(recordArr);
            recordArrayList.add(record);
        }
        return recordArrayList;
    }

    /**
     * This method checks if a string is an integer or not
     * @param str string needs to be checks
     * @return true if a string is integer, false if not
     */
    public static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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
}
