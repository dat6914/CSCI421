package Main;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This Class is to store the schema of database
 */
public class Catalog {
    private ArrayList<Table> tables_list = new ArrayList<>();
    private String DBlocation;

    public Catalog(String DBlocation) {
        this.DBlocation = DBlocation;
        this.tables_list = new ArrayList<>();
        this.tables_list = tableListFromSchema();
    }


    /**
     * Method converts catalog to byte array
     * @param catalog catalog need to be converted to byte array
     * @return byte array of catalog
     */
    public byte[] convertCatalogToByteArr(Catalog catalog) {
        ArrayList<Table> table_list = catalog.getTablesList();
        int tableNum = table_list.size();
        ByteBuffer result = ByteBuffer.allocate(0);
        byte[] tableNumArr = ByteBuffer.allocate(4).putInt(tableNum).array();
        result = appendByteBuffer(result, tableNumArr);
        for (Table tbl : table_list) {
            byte[] tableByteArr = convertTableToByteArr(tbl);
            result = appendByteBuffer(result, tableByteArr);
        }
        return result.array();
    }


    /**
     * Method gets tables list
     * @return table list
     */
    public ArrayList<Table> getTablesList() {
        return this.tables_list;
    }


    /**
     * Method reads the Main.Catalog.txt and return the byte array
     * @param path path of Main.Catalog.txt
     * @return byte array
     */
    public static byte[] readCatalogFile(String path) {
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
     * Method displays the display schema
     * @param location location of database
     * @param pageSize size of page (byte)
     * @param bufferSize    size of buffer (number of pages)
     * @param catalog catalog needs to be displayed
     */
    public void displaySchema(String location, int pageSize, int bufferSize, Catalog catalog) {
        System.out.println("DB location: " + location);
        System.out.println("Main.Page Size: " + pageSize);
        System.out.println("Buffer Size: " + bufferSize);
        System.out.println("Tables: \n");

        if (catalog.getTablesList().size() != 0) {
            for (Table table : catalog.getTablesList()) {
                System.out.println(tableToString(table));
            }
            System.out.println("SUCCESS");
        } else {
            System.out.println("No tables to display");
            System.out.println("SUCCESS");
        }
    }


    /**
     * Method prints the table out
     * @param table table need to be printed out
     */
    public void displayInfoTable(Table table) {
        String str = tableToString(table);
        System.out.println(str);
    }


    /**
     * Method returns the arraylist of tables from catalog
     * @return arraylist of tables
     */
    public ArrayList<Table> tableListFromSchema() {
        String catalogPath = this.DBlocation + "/catalog.txt";
        File file = new File(catalogPath);
        if (file.exists()) {
            byte[] catalogByteArr = readCatalogFile(catalogPath);
            return convertByteArrToCatalog(catalogByteArr);
        } else {
            return new ArrayList<>();
        }
    }


    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6} AttributeType
    //  enum: {primary 0 1}

    /**
     * Method returns the string of a table
     * @param table table
     * @return string of table
     */
    public static String tableToString(Table table) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Main.Table Name: ");
        stringBuilder.append(table.getTableName()).append("\n");
        stringBuilder.append("Main.Table schema: \n");
        ArrayList<String> attriNameList = table.getAttriName_list();
        ArrayList<String> attriTypeList = table.getAttriType_list();
        String primarykeyName = table.getPrimaryKeyName();
        for (int i = 0; i < attriNameList.size(); i++) {
            stringBuilder.append("\t");
            stringBuilder.append(attriNameList.get(i));
            stringBuilder.append(":");
            char attriType = attriTypeList.get(i).charAt(0);
            if (attriType == '2') {
                stringBuilder.append("boolean");
            } else if (attriType == '3') {
                stringBuilder.append("integer");
            } else if (attriType == '4') {
                stringBuilder.append("double");
            } else if (attriType == '5') {
                stringBuilder.append("char(");
                String temp = attriTypeList.get(i).substring(1);
                stringBuilder.append(temp);
                stringBuilder.append(")");
            } else if (attriType == '6') {
                stringBuilder.append("varchar(");
                String temp = attriTypeList.get(i).substring(1);
                stringBuilder.append(temp);
                stringBuilder.append(")");
            } else {
                System.out.println("Something goes wrong while converting table to string!");
                System.err.println("ERROR");
                return null;
            }
            if (primarykeyName.equals(attriNameList.get(i))) {
                stringBuilder.append(" primarykey ");
            }
            stringBuilder.append("\n");
        }
        stringBuilder.append("Pages: ");
        stringBuilder.append(table.getPageID_list().size()).append("\n");
        stringBuilder.append("Records: ");
        stringBuilder.append(table.getRecordNumUpdate()).append("\n");

        return stringBuilder.toString();
    }


    /**
     * Method converts byte array of catalog to arraylist of tables
     * @param catalogByteArr byte array of catalog
     * @return arraylist of tables
     */
    public ArrayList<Table> convertByteArrToCatalog(byte[] catalogByteArr) {
        ArrayList<Table> result = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(catalogByteArr);
        int tableNum = byteBuffer.getInt(0);
        int indexTracking = 4;
        for (int i = 0; i < tableNum; i++) {
            byte[] tableSizeArr = Arrays.copyOfRange(catalogByteArr, indexTracking, indexTracking + 4);
            int tableSize = ByteBuffer.wrap(tableSizeArr).getInt();
            indexTracking = indexTracking + 4;

            byte[] attriNameArr = Arrays.copyOfRange(catalogByteArr, indexTracking, indexTracking + tableSize);
            indexTracking = indexTracking + tableSize;
            Table table = convertByteArrToTable(attriNameArr);
            result.add(table);
        }

        return result;
    }

    //FORMAT: student name varchar(15) studentID integer primarykey address char(20) gpa double incampus boolean

    /**
     * Methods converts byte array of table to table object
     * @param tableArr byte array of table
     * @return table object
     */
    public Table convertByteArrToTable(byte[] tableArr) {

        ByteBuffer result = ByteBuffer.wrap(tableArr);
        // get 4 bytes of tableName size
        int tableNameSize = result.getInt(0);
        byte[] tableNameArr = Arrays.copyOfRange(tableArr, 4, tableNameSize + 4);

        //Variables need for create a table
        String tableName = new String(tableNameArr, StandardCharsets.UTF_8);
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();
        ArrayList<Integer> pageIDList = new ArrayList<>();

        // get next 4 bytes for the number of attributes
        byte[] numberAttributes = Arrays.copyOfRange(tableArr, 4 + tableNameSize, 4 + tableNameSize + 4);
        int attributeNum = ByteBuffer.wrap(numberAttributes).getInt();
        int indexTracking = 4 + tableNameSize + 4;

        // get all the attributes
        for (int i = 0; i < attributeNum; i++) {
            // get next 4 bytes for the attribute name size
            byte[] attriNameSizeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
            int attriNameSize = ByteBuffer.wrap(attriNameSizeArr).getInt();
            indexTracking = indexTracking + 4;

            // get the next attriNameSize for the name of attribute and add to the attribute name list
            byte[] attriNameArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + attriNameSize);
            indexTracking = indexTracking + attriNameSize;
            String attrName = new String(attriNameArr, StandardCharsets.UTF_8);
            attriNameList.add(attrName);

            // now for attribute type
            StringBuilder attriTypeStrBuilder = new StringBuilder();
            // get next 4 bytes for attribute type. It will follow the hard set up:
            //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
            byte[] attriTypeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
            int attriType = ByteBuffer.wrap(attriTypeArr).getInt();
            indexTracking = indexTracking + 4;
            attriTypeStrBuilder.append(attriType);

            // get next 4 bytes for attribute type size.
            // by setup, 0 if integer, boolean, or double, and the size for varchar and char
            byte[] attriTypeSizeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
            int attriTypeSize = ByteBuffer.wrap(attriTypeSizeArr).getInt();
            indexTracking = indexTracking + 4;
            attriTypeStrBuilder.append(attriTypeSize);

            // get next 4 bytes for primary key by hard setup:
            //  enum: {primary 0 1}
            byte[] primarykey = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
            int primaryNum = ByteBuffer.wrap(primarykey).getInt();
            indexTracking = indexTracking + 4;
            if (primaryNum == 1) {
                primarykeyName = attriNameList.get(attriNameList.size() - 1);
            }

            // only add the attribute type (first digit) and the size of attribute type (from the 2nd digit)
            attriTypeList.add(attriTypeStrBuilder.toString());

        }

        byte[] pageNumArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
        int numPage = ByteBuffer.wrap(pageNumArr).getInt();
        indexTracking = indexTracking + 4;
        for (int i = 0; i < numPage; i++) {
            byte[] pageIDArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking + 4);
            int pageID = ByteBuffer.wrap(pageIDArr).getInt();
            indexTracking = indexTracking + 4;
            pageIDList.add(pageID);
        }

        if (attriNameList.size() != attriTypeList.size()) {
            System.err.println("Something goes wrong. Can't read the catalog file.");
            System.err.println("ERROR");
            return null;
        }

        boolean checkPrimary = false;
        for (String i : attriNameList) {
            if (primarykeyName.equalsIgnoreCase(i)) {
                checkPrimary = true;
                break;
            }
        }
        if (!checkPrimary) {
            System.err.println("Something goes wrong. primarykey is not in the attribute list!");
            System.err.println("ERROR");
            return null;
        }

        Table newTable = new Table(tableName, primarykeyName, attriNameList, attriTypeList, this.DBlocation, pageIDList);

        return newTable;
    }


    // FORMAT: student name varchar(15) studentID integer primarykey address char(20) gpa double incampus boolean
    // Byte[]:  first 4 bytes for the size of whole string HERE: X  = 130   TOTAL : 130 + 4
    //          next 4 bytes store the size of tableName HERE: 7
    //          next 7 bytes store "student"
    //          next 4 bytes store the number of attributes HERE: 5
    //      ------------------------------------------------------------
    //          next 4 bytes store the size of name of A1 HERE: 4
    //          next 4 bytes store A1 Name HERE: "name"
    //          next 4 bytes store A1 type HERE: 6  because "varchar(15)" = 6 by setup
    //          next 4 bytes store A1 type size HERE: 15 because it is a 6 and 15 varchar
    //          next 4 bytes store 1 if a primarykey and 0 if not HERE: 0
    //      -------------------------------------------------------------------
    //          next 4 bytes store the size of name of A2 HERE: 9
    //          next 9 bytes store A2 Name HERE: "studentID"
    //          next 4 bytes store A2 type HERE: 3  because "integer" = 3 by setup
    //          next 4 bytes store A2 type size HERE: 0 because it is a 3 and has no size
    //          next 4 bytes store 1 if a primarykey and 0 if not HERE: 1
    //      -------------------------------------------------------------------
    //          next 4 bytes store the size of name of A3 HERE: 7
    //          next 7 bytes store A3 Name HERE: "address"
    //          next 4 bytes store A3 type HERE: 5  because "char(20)" = 5 by setup
    //          next 4 bytes store A3 type size HERE: 20 because it is a 5 and 20 char
    //          next 4 bytes store 1 if a primarykey and 0 if not HERE: 0
    //      -------------------------------------------------------------------
    //          next 4 bytes store the size of name of A4 HERE: 3
    //          next 3 bytes store A4 Name HERE: "gpa"
    //          next 4 bytes store A4 type HERE: 4  because "double" = 4 by setup
    //          next 4 bytes store A4 type size HERE: 0 because it is a 4 and has no sise
    //          next 4 bytes store 1 if a primarykey and 0 if not HERE: 0
    //      -------------------------------------------------------------------
    //          next 4 bytes store the size of name of A5 HERE: 8
    //          next 8 bytes store A5 Name HERE: "incampus"
    //          next 4 bytes store A5 type HERE: 2  because "boolean" = 2 by setup
    //          next 4 bytes store A5 type size HERE: 0 because it is a 2 and has no size
    //          next 4 bytes store 1 if a primarykey and 0 if not HERE: 0
    //     ----------------------------------------------------------------------
    //  Total size of this table is : 19 + 20 + 25 + 23 + 19 + 24 = 130

    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
    //  enum: {primary 0 1}

    /**
     * Method converts table object to byte array of table
     * @param table table object
     * @return byte array of table object
     */
    public byte[] convertTableToByteArr(Table table) {

        ByteBuffer result = ByteBuffer.allocate(0);
        // encoding the tableName
        int tableNameSize = table.getTableName().length();
        byte[] tempNameSize = ByteBuffer.allocate(4).putInt(tableNameSize).array();
        byte[] tableNameArr = table.getTableName().getBytes(StandardCharsets.UTF_8);
        result = appendByteBuffer(result, tempNameSize);
        result = appendByteBuffer(result, tableNameArr);
        // encoding the number of attributes
        int numberAttriName = table.getAttriName_list().size();
        byte[] numAttriName = ByteBuffer.allocate(4).putInt(numberAttriName).array();
        result = appendByteBuffer(result, numAttriName);
        // encoding each of attributes: attributeName and attributesType
        for (int i = 0; i < numberAttriName; i++) {
            // attributeName
            int attriNameSize = table.getAttriName_list().get(i).length();
            byte[] attriNameSizeArr = ByteBuffer.allocate(4).putInt(attriNameSize).array();
            byte[] attriNameArr = table.getAttriName_list().get(i).getBytes(StandardCharsets.UTF_8);
            result = appendByteBuffer(result, attriNameSizeArr);
            result = appendByteBuffer(result, attriNameArr);

            // attributeType
            char firstChar = table.getAttrType(i).charAt(0);
            int attriType = Integer.parseInt(String.valueOf(firstChar));
            byte[] attriTypeArr = ByteBuffer.allocate(4).putInt(attriType).array();
            result = appendByteBuffer(result, attriTypeArr);

            int tempForTypeSize = 0;
            if (attriType == 2 || attriType == 3 || attriType == 4) {
            } else {
                String typeSize = table.getAttrType(i).substring(1);
                try {
                    tempForTypeSize = Integer.parseInt(typeSize);
                } catch (NumberFormatException ex) {
                    System.err.println("Something is wrong in Main.Table");
                    ex.printStackTrace();
                    System.err.println("ERROR");
                    return null;
                }
            }
            byte[] attriTypeSizeArr = ByteBuffer.allocate(4).putInt(tempForTypeSize).array();
            result = appendByteBuffer(result, attriTypeSizeArr);

            int primary = 0;
            if (table.getPrimaryKeyName().equals(table.getAttriName_list().get(i))) {
                primary = 1;
            }
            byte[] primarkeyArr = ByteBuffer.allocate(4).putInt(primary).array();
            result = appendByteBuffer(result, primarkeyArr);
        }

        ArrayList<Integer> pageIDList = table.getPageID_list();
        int numPage = pageIDList.size();
        byte[] numPageArr = ByteBuffer.allocate(4).putInt(numPage).array();
        result = appendByteBuffer(result, numPageArr);

        for (Integer integer : pageIDList) {
            byte[] temp = ByteBuffer.allocate(4).putInt(integer).array();
            result = appendByteBuffer(result, temp);
        }

        // get the total size of table
        int resultSize = result.array().length;
        ByteBuffer temp = ByteBuffer.allocate(4).putInt(resultSize);
        result = appendByteBuffer(temp, result.array());

        return result.array();

    }


    /**
     * Method appends byte buffer
     * @param current current byte buffer
     * @param arr byte arry
     * @return the byte buffer that appends the byte array
     */
    public static ByteBuffer appendByteBuffer(ByteBuffer current, byte[] arr) {
        ByteBuffer result = ByteBuffer.allocate(current.capacity() + arr.length);
        result.put(current.array(), 0, current.array().length);
        result.put(arr, 0, arr.length);
        return result;
    }

}