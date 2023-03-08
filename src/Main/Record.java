package Main;

import org.w3c.dom.Attr;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This class is to store record's values
 */
public class Record {

    private ArrayList<AttributeInfo> attributeInfoList;

    private ArrayList<Object> valuesList;

    /**
     * Constructor of Main.Record
     * @param valuesList the arraylist of values
     */
    public Record(ArrayList<Object> valuesList, ArrayList<String> attributeList) {
        this.valuesList = valuesList;
        this.attributeInfoList = calculateAttributeInfo(attributeList);

    }

    /**
     * Method gets the list of values
     * @return arraylist Object of values
     */
    public ArrayList<Object> getValuesList() {
        return valuesList;
    }

    public ArrayList<AttributeInfo> getAttributeInfoList(){
        return attributeInfoList;
    }

    /**
     * This method converts record object of a table to byte[]
     * @param record record object need to be converted to byte[]
     * @param table table object that the record belongs to
     * @return byte[] of record including checking attribute types
     */
    public byte[] convertRecordToByteArr(Record record, Table table) {



        ArrayList<Object> valuesList = record.getValuesList();
        ArrayList<String> attrTypeList = constructTypeList(record.getAttributeInfoList());

        ByteBuffer result = ByteBuffer.allocate(0);
        int numValue = valuesList.size();
        byte[] numValueArr = ByteBuffer.allocate(Integer.BYTES).putInt(numValue).array();
        result = Page.appendByteBuffer(result, numValueArr);

        if (attrTypeList.size() != valuesList.size()) {
            System.err.println("Too many attributes!");
            System.err.println("ERROR");
            return null;
        }

        for (int i = 0; i < valuesList.size(); i++) {
            Object temp = valuesList.get(i);
            AttributeInfo attributeInfo = attributeInfoList.get(i);
            byte[] attributeInfoByteArray = attributeInfo.serializeAttributeInfo();
            result = Page.appendByteBuffer(result, attributeInfoByteArray);


            //char valueType = attrTypeList.get(i).charAt(0);
            String valueType = attributeInfo.getType();

            if (Objects.equals(valueType, "2") && temp instanceof Boolean) {
                boolean bo = (Boolean) temp;
                byte[] valueSizeArr = ByteBuffer.allocate(1).put((byte) (bo ? 1 : 0)).array();
                result = Page.appendByteBuffer(result, valueSizeArr);
            } else if (Objects.equals(valueType, "3") && temp instanceof Integer) {
                int in = (Integer) temp;
                byte[] valueSizeArr = ByteBuffer.allocate(Integer.BYTES).putInt(in).array();
                result = Page.appendByteBuffer(result, valueSizeArr);
            } else if (Objects.equals(valueType, "4") && temp instanceof Double) {
                double dou = (Double) temp;
                byte[] valueArray = ByteBuffer.allocate(Double.BYTES).putDouble(dou).array();
                result = Page.appendByteBuffer(result, valueArray);
            } else if (Objects.equals(valueType, "5") && temp instanceof String) {
                String stringSize = attrTypeList.get(i).substring(1);
                int size = 0;
                if (Page.isInteger(stringSize)) {
                    size = Integer.parseInt(stringSize);
                }
                int stringLength = ((String) temp).length();
                if (stringLength < size - 1) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength + 1).array();
                    result = Page.appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.allocate(strArr.length + 1);
                    buffer.put(strArr);
                    buffer.put((byte) 0);
                    result = Page.appendByteBuffer(result, buffer.array());

                } else if (stringLength == size) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength).array();
                    result = Page.appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    result = Page.appendByteBuffer(result, strArr);
                } else {
                    System.err.println("Size char of " + stringSize + " but got string size: " + stringLength);
                    System.err.println("ERROR");
                    return null;
                }
            } else if (Objects.equals(valueType, "6") && temp instanceof String) {
                String stringSize = attrTypeList.get(i).substring(1);
                int size = 0;
                if (Page.isInteger(stringSize)) {
                    size = Integer.parseInt(stringSize);
                }
                int stringLength = ((String) temp).length();
                if (stringLength < size - 1) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength + 1).array();
                    result = Page.appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.allocate(strArr.length + 1);
                    buffer.put(strArr);
                    buffer.put((byte) 0);
                    result = Page.appendByteBuffer(result, buffer.array());

                } else if (stringLength == size) {
                    byte[] sizeString = ByteBuffer.allocate(4).putInt(stringLength).array();
                    result = Page.appendByteBuffer(result, sizeString);
                    byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8);
                    result = Page.appendByteBuffer(result, strArr);
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
        result = Page.appendByteBuffer(temp, result.array());

        return result.array();
    }

    /**
     * This method converts byte array of record in a table to record object
     * @param record byte array of record
     * @param table table that record belongs to
     * @return record object
     */
    public static Record convertByteArrToRecord(byte[] record, Table table) {
        ArrayList<Object> valuesList = new ArrayList<>();
        ArrayList<String> attrTypeList = new ArrayList<>();

        ByteBuffer result = ByteBuffer.wrap(record);
        int recordSize = result.getInt(0);
        int numRecord = result.getInt(4);
        if (attrTypeList.size() != numRecord) {
            System.err.println("Something goes wrong in converting byte[] to Main.Record");
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
                byte[] valueArr = Arrays.copyOfRange(record, indexTracking, indexTracking + Integer.BYTES);
                Integer in = ByteBuffer.wrap(valueArr).getInt();
                indexTracking = indexTracking + Integer.BYTES;
                valuesList.add(in);
            } else if (attrType == '4') {
                byte[] valueArr = Arrays.copyOfRange(record, indexTracking, indexTracking + Double.BYTES);
                Double dou = ByteBuffer.wrap(valueArr).getDouble();
                indexTracking = indexTracking + Double.BYTES;
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
                System.err.println("Can't convert byte[] to Main.Record");
                System.err.println("ERROR");
                return null;
            }
        }
        Record res = new Record(valuesList, new ArrayList<String>());
        return res;
    }

    public ArrayList<AttributeInfo> calculateAttributeInfo(ArrayList<String> attributeList){
        ArrayList<AttributeInfo> attributeInfoList = new ArrayList<AttributeInfo>();
        for(int i = 0; i < valuesList.size(); i++){
            String type = attributeList.get(i).substring(0,1);
            switch(type){
                case "2" : attributeInfoList.add(new AttributeInfo(type, 1));
                    break;
                case "3" : attributeInfoList.add(new AttributeInfo(type, Integer.BYTES));
                    break;
                case "4" : attributeInfoList.add(new AttributeInfo(type, Double.BYTES));
                    break;
                case "5" : int length = Integer.parseInt(attributeList.get(i).substring(1));
                    attributeInfoList.add(new AttributeInfo(type, length));
                    break;
                case "6" : String s = (String) valuesList.get(i);
                    int len = s.length();
                    attributeInfoList.add(new AttributeInfo(type, len));
                }
            }
        return attributeInfoList;
        }


    public static ArrayList<String> constructTypeList(ArrayList<AttributeInfo> attributeInfoList){
        ArrayList<String> typeList = new ArrayList<>();
        for(AttributeInfo ai: attributeInfoList){
            typeList.add(ai.getType());
        }

        return typeList;
    }


    }


