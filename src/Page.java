import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;


public class Page {
    private int pageID;
    private ArrayList<Record> record_list = new ArrayList<>();
    private ArrayList<Object> primarykeyValueList = new ArrayList<>();
    private Table table;
    private int current_page_size;
    private String DBLocation;

    public Page(int pageID, Table table, String DBLocation) {
        this.pageID = pageID;
        this.table = table;
        this.DBLocation = DBLocation;
        this.record_list = getRecordListFromPage(pageID, table);
        this.current_page_size = computeCurrentPagesize(record_list);
    }

    public Table getTable() {
        return this.table;
    }

    public int getCurrent_page_size() {
        return this.current_page_size;
    }

    public int computeCurrentPagesize(ArrayList<Record> record_list) {
        int result = 0;
        result = result + 4;
        for (Record record : record_list) {
            result = result + record.getValuesList().size();
        }

        result = result + record_list.size()*8;

        return result;
    }

    public void incCurrentPageSize(Record record){
        this.current_page_size = this.current_page_size + record.getValuesList().size() + 8;
    }

    public int getPageID() {
        return this.pageID;
    }


    public ArrayList<Record> getRecordList() {
        return this.record_list;
    }

    public ArrayList<Record> getRecordListFromPage(int pageID, Table table){
        String pagePath = this.DBLocation + "/Pages/" + pageID + ".txt";
        File file = new File(pagePath);
        if (file.exists()) {
            byte[] pageArr = readPageFile(pagePath);
            return convertByteArrToPage(pageArr, table);
        } else {
            return new ArrayList<>();
        }
    }

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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Page)
            return ((Page) obj).pageID == pageID;
        return false;
    }

    public byte[] convertPageToByteArr(ArrayList<Record> record_list, Table table, int page_size) {
        //int currentPageSize = 4;
        int indextracking = 0;
        int numRecord = record_list.size();
        byte[] result = new byte[page_size];
        byte[] numRecordArr = ByteBuffer.allocate(4).putInt(numRecord).array();
        System.arraycopy(numRecordArr, 0, result, indextracking, numRecordArr.length);
        //currentPageSize = currentPageSize + numRecordArr.length;
        indextracking = indextracking + numRecordArr.length;
        int indexReverse = page_size;
        for (int i = 0; i < numRecord; i++) {
            Record record = record_list.get(i);
            byte[] recordArr = convertRecordToByteArr(record, table);
            int length = recordArr.length;
            indexReverse = indexReverse - length;
            byte[] offSetArr = ByteBuffer.allocate(4).putInt(indexReverse).array();
            System.arraycopy(offSetArr, 0, result, indextracking, offSetArr.length);
            //urrentPageSize = currentPageSize + offSetArr.length;
            indextracking = indextracking + 4;

            byte[] lengthArr = ByteBuffer.allocate(4).putInt(length).array();
            System.arraycopy(lengthArr, 0, result, indextracking, lengthArr.length);
            //currentPageSize = currentPageSize + lengthArr.length;
            indextracking = indextracking + 4;

            System.arraycopy(recordArr, 0, result, indexReverse , recordArr.length);
            //currentPageSize = currentPageSize + recordArr.length;
        }
        //byte[] curPageSize = ByteBuffer.allocate(4).putInt(currentPageSize).array();
        //System.arraycopy(curPageSize, 0, result, 0, curPageSize.length);

//        if (currentPageSize > page_size) {
//            System.err.println("The records size is more than page size");
//            System.err.println("ERROR");
//            return null;
//        }
        return result;
    }

    public ArrayList<Record> convertByteArrToPage(byte[] byteArr, Table table) {
        ArrayList<Record> recordArrayList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArr);
        //int pageSize = byteBuffer.getInt(0);
        int recordNum = byteBuffer.getInt(0);
        int indextracking = 4;

        for (int i = 0; i < recordNum; i++) {
            int offset = byteBuffer.getInt(indextracking);
            indextracking = indextracking + 4;
            int length = byteBuffer.getInt(indextracking);
            indextracking = indextracking + 4;

            byte[] recordArr = Arrays.copyOfRange(byteArr, offset, offset+length);
            Record record = convertByteArrToRecord(recordArr, table);
            recordArrayList.add(record);
        }

        return recordArrayList;
    }


    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
    //  "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    //FORMAT: ("Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true)
    //  "Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true
    /**
     * This method converts byte array of record in a table to record object
     * @param record byte array of record
     * @param table table that record belongs to
     * @return record object
     */
    public Record convertByteArrToRecord(byte[] record, Table table) {
        ArrayList<Object> valuesList = new ArrayList<>();
        ArrayList<String> attrTypeList = table.getAttriType_list();

        ByteBuffer result = ByteBuffer.wrap(record);
        int recordSize = result.getInt(0);
        int numRecord = result.getInt(4);
        if (attrTypeList.size() != numRecord) {
            System.err.println("Something goes wrong in converting byte[] to Record");
            System.err.println("ERROR");
            return null;
        }
        int indexTracking = 8;
        for (int i = 0; i < numRecord; i++) {

            char attrType = attrTypeList.get(i).charAt(0);
            if (attrType == '2') {
                byte[] valueArr = Arrays.copyOfRange(record, indexTracking, indexTracking + 1);
                Boolean bo = ByteBuffer.wrap(valueArr).get() != 0;
                indexTracking = indexTracking + 1;
                valuesList.add(bo);
            } else if (attrType == '3') {
                byte[] valueArr = Arrays.copyOfRange(record, indexTracking, indexTracking + 4);
                Integer in = ByteBuffer.wrap(valueArr).getInt();
                indexTracking = indexTracking + 4;
                valuesList.add(in);
            } else if (attrType == '4') {
                byte[] valueArr = Arrays.copyOfRange(record, indexTracking, indexTracking + 8);
                Double dou = ByteBuffer.wrap(valueArr).getDouble();
                indexTracking = indexTracking + 8;
                valuesList.add(dou);
            } else if (attrType == '5') {
                byte[] sizeString = Arrays.copyOfRange(record, indexTracking, indexTracking + 4);
                int size = ByteBuffer.wrap(sizeString).getInt();
                indexTracking = indexTracking + 4;
                byte[] strArr = Arrays.copyOfRange(record, indexTracking, indexTracking + size);
                indexTracking = indexTracking + size;
                String value = new String(strArr, StandardCharsets.UTF_8);
                valuesList.add(value);

            } else if (attrType == '6') {
                byte[] sizeString = Arrays.copyOfRange(record, indexTracking, indexTracking + 4);
                int size = ByteBuffer.wrap(sizeString).getInt();
                indexTracking = indexTracking + 4;
                byte[] strArr = Arrays.copyOfRange(record, indexTracking, indexTracking + size);
                indexTracking = indexTracking + size;
                String value = new String(strArr, StandardCharsets.UTF_8);
                valuesList.add(value);
            } else {
                System.err.println("Can't convert byte[] to Record");
                System.err.println("ERROR");
                return null;
            }
        }

        Record res = new Record(valuesList);
        return res;
    }


    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
    //  "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    //  FORMAT: ("Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true)
    //  "Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true
    /**
     * This method convert record object of a table to byte[]
     * @param record record object need to be converted to byte[]
     * @param table table object that the record belongs to
     * @return byte[] of record including checking attribute types
     */
    public byte[] convertRecordToByteArr(Record record, Table table) {
        ArrayList<Object> valuesList = record.getValuesList();
        ArrayList<String> attrTypeList = table.getAttriType_list();
        ByteBuffer result = ByteBuffer.allocate(0);
        int numValue = valuesList.size();
        byte[] numValueArr = ByteBuffer.allocate(4).putInt(numValue).array();
        result = appendByteBuffer(result, numValueArr);

        if (attrTypeList.size() != valuesList.size()) {
            System.err.println("Too many attributes!");
            System.err.println("ERROR");
            return null;
        }

        for (int i = 0; i < valuesList.size(); i++) {
            Object temp = valuesList.get(i);
            char valueType = attrTypeList.get(i).charAt(0);
            if (valueType == '2' && temp instanceof Boolean) {
                boolean bo = (Boolean) temp;
                byte[] valueSizeArr = ByteBuffer.allocate(1).put((byte) (bo ? 1 : 0)).array();
                result = appendByteBuffer(result, valueSizeArr);
            } else if (valueType == '3' && temp instanceof Integer) {
                int in = (Integer) temp;
                byte[] valueSizeArr = ByteBuffer.allocate(4).putInt(in).array();
                result = appendByteBuffer(result, valueSizeArr);
            } else if (valueType == '4' && temp instanceof Double) {
                double dou = (Double) temp;
                byte[] valueArray = ByteBuffer.allocate(8).putDouble(dou).array();
                result = appendByteBuffer(result, valueArray);
            } else if (valueType == '5' && temp instanceof String) {
                String stringSize = attrTypeList.get(i).substring(1);
                int size = 0;
                if (isInteger(stringSize)) {
                    size = Integer.parseInt(stringSize);
                }
                int stringLength = ((String) temp).length();
                if (stringLength < size - 1) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength + 1).array();
                    result = appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.allocate(strArr.length + 1);
                    buffer.put(strArr);
                    buffer.put((byte) 0);
                    result = appendByteBuffer(result, buffer.array());

                } else if (stringLength == size) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength).array();
                    result = appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    result = appendByteBuffer(result, strArr);
                } else {
                    System.err.println("Size char of " + stringSize + " but got string size: " + stringLength);
                    System.err.println("ERROR");
                    return null;
                }
            } else if (valueType == '6' && temp instanceof String) {
                String stringSize = attrTypeList.get(i).substring(1);
                int size = 0;
                if (isInteger(stringSize)) {
                    size = Integer.parseInt(stringSize);
                }
                int stringLength = ((String) temp).length();
                if (stringLength < size - 1) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength + 1).array();
                    result = appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.allocate(strArr.length + 1);
                    buffer.put(strArr);
                    buffer.put((byte) 0);
                    result = appendByteBuffer(result, buffer.array());

                } else if (stringLength == size) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength).array();
                    result = appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    result = appendByteBuffer(result, strArr);
                } else {
                    System.err.println("Size varchar of " + stringSize + " but got string size: " + stringLength);
                    System.err.println("ERROR");
                    return null;
                }
            } else {
                System.err.println("There is no data type: " + temp.getClass().getName());
                System.err.println("ERROR");
                return null;
            }
        }

        // encode the total size of record
        int resultSize = result.array().length;
        ByteBuffer temp = ByteBuffer.allocate(4).putInt(resultSize);
        result = appendByteBuffer(temp, result.array());

        return result.array();
    }

    /**
     * This method checks if a string is an integer or not
     * @param str string needs to be checks
     * @return true if a string is integer, false if not
     */
    private static boolean isInteger(String str) {
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