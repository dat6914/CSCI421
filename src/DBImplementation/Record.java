package DBImplementation;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class Record {
    /**
     *
     */
    private ArrayList<Object> valuesList;

    private String primaryKey;
    /**
     * Constructor of DBImplementation.Record
     * @param valuesList the arraylist of values
     */
    public Record(ArrayList<Object> valuesList)
    {
        this.valuesList = valuesList;
    }

    /**
     * Method gets the list of values
     * @return arraylist Object of values
     */
    public ArrayList<Object> getValuesList() {
        return valuesList;
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
        ArrayList<String> attrTypeList = new ArrayList<>();
        //todo stub ArrayList<String> attrTypeList = table.getAttriType_list();
        ByteBuffer result = ByteBuffer.wrap(record);
        int recordSize = result.getInt(0);
        int numRecord = result.getInt(4);
        if (attrTypeList.size() != numRecord) {
            System.err.println("Something goes wrong in converting byte[] to DBImplementation.Record");
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
                System.err.println("Can't convert byte[] to DBImplementation.Record");
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
        ArrayList<String> attrTypeList = new ArrayList<>();
        //todo stub ArrayList<String> attrTypeList = table.getAttriType_list();

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

    public String getPrimaryKey(){
        return primaryKey;
    }

}
