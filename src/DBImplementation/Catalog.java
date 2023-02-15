package DBImplementation;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Catalog {
    private ArrayList<Table> tables_list = new ArrayList<>();
    private String DBlocation;
    private HashMap<String, ArrayList<String>> tableTypeMap = new HashMap<>();
    private HashMap<String, Table> tableMap = new HashMap<>();

    public Catalog(String DBlocation) {
        this.DBlocation = DBlocation;
        this.tables_list = tableListFromSchema();

    }

    public ArrayList<Table> getTablesList() {
        return tables_list;
    }

    public HashMap<String, Table> getTableMap(){
        return tableMap;
    }

    public static byte[] readCatalogFile(String path) {
        try {
            File file = new File(path);
            FileInputStream fs = new FileInputStream(file);
            byte[] arr = new byte[(int) file.length()];
            fs.read(arr);
            fs.close();
            //System.out.println("SUCCESS");
            return arr;
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file.");
            e.printStackTrace();
            System.err.println("ERROR");
            return null;
        }
    }

    public void displaySchema(String location, int pageSize, int bufferSize) {
        System.out.println("DB location: " + location);
        System.out.println("Page Size: " + pageSize);
        System.out.println("Buffer Size: " + bufferSize);
        System.out.println("Tables: \n");

        ArrayList<Table> tableArrayListFromCATALOGFILE = tableListFromSchema();
        if (tableArrayListFromCATALOGFILE != null) {
            for (Table table : tableArrayListFromCATALOGFILE) {
                System.out.println(tableToString(table));
            }
            System.out.println("SUCCESS");
        }
        if (tableArrayListFromCATALOGFILE.size() == 0) {
            System.out.println("No tables to display in schema");
            System.out.println("SUCCESS");
        }
    }

    public void displayInfoTable(String tableName) {
        if (this.tables_list.size() != 0) {
            boolean flag = false;
            for (Table table : this.tables_list) {
                if (table.getTableName().equals(tableName)) {
                    System.out.println(tableToString(table));
                    System.out.println("SUCCESS");
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                System.out.println("No such table " + tableName);
                System.out.println("ERROR");
            }
        } else {
            System.out.println("No such table " + tableName);
            System.out.println("ERROR");
        }
    }

    public ArrayList<Table> tableListFromSchema() {
        String catalogPath = DBlocation + "/catalog.txt";
        File file = new File(catalogPath);
        if (file.exists()) {
            byte[] catalogByteArr = readCatalogFile(catalogPath);
            ArrayList<Table> tableFromSchema = convertByteArrToCatalog(catalogByteArr);
            return tableFromSchema;
        } else {
            ArrayList<Table> res = new ArrayList<>();
            return res;
        }
    }

    public Table getTableForInsert(String tableName) {
        ArrayList<Table> tableArrayList = tableListFromSchema();
        for (Table table : tableArrayList) {
            if (table.getTableName().equals(tableName)) {
                return table;
            } else {
                System.err.println("No such table " + tableName);
                return null;
            }
        }
        System.err.println("No such table " + tableName);
        return null;
    }


    // "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    // FORMAT: insert into student values ("Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true),("(A)" 1 "school" 2 "(false")
    // check: many tuples in 1 sql, check duplicate primary, check how many attributes, check the type of attribute,
    // check the length in varchar and char, check null, check if table name is exist
    public ArrayList<Record> checkInsertRecordSQL(String inp, Table table) {
        int startIndex = inp.indexOf("(");
        String inputTupesList = inp.substring(startIndex);




        String primaryKey = table.getPrimaryKeyName();
        ArrayList<String> attriNameList = table.getAttriName_list();
        //todo table does not have attributeTypeList ArrayList<String> attriTypeList = table.getAttriType_list();



        return null;
    }


    /**
     * Method removes element of String array
     *
     * @param old   the current String array
     * @param index index of element that will be removed
     * @return String array after remove
     */
    private String[] removeElementInStringArray(String[] old, int index) {
        String[] newArray = new String[old.length - 1];
        for (int i = 0, j = 0; i < old.length; i++) {
            if (i != index) {
                newArray[j++] = old[i];
            }
        }
        return newArray;
    }

    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6} AttributeType
    //  enum: {primary 0 1}
    public static String tableToString(Table table) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Table Name: ");
        stringBuilder.append(table.getTableName() + "\n");
        stringBuilder.append("Table schema: \n");
        ArrayList<String> attriNameList = table.getAttriName_list();
        ArrayList<String> attriTypeList = new ArrayList<>();
        //ArrayList<String> attriTypeList = table.getAttriType_list();
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
        // todod stub stringBuilder.append(table.getRecordNum()).append("\n");

        return stringBuilder.toString();
    }


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

        Table newTable = new Table(tableName, primarykeyName, attriNameList, attriTypeList);
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
            char firstChar = 'a';
            //todo stub.char firstChar = table.getAttrType(i).charAt(0);
            int attriType = Integer.parseInt(String.valueOf(firstChar));
            byte[] attriTypeArr = ByteBuffer.allocate(4).putInt(attriType).array();
            result = appendByteBuffer(result, attriTypeArr);

            int tempForTypeSize = 0;
            if (attriType == 2 || attriType == 3 || attriType == 4) {
                tempForTypeSize = 0;
            } else {
                String typeSize = "a";
                //todo stub. String typeSize = table.getAttrType(i).substring(1);
                try {
                    tempForTypeSize = Integer.parseInt(typeSize);
                } catch (NumberFormatException ex) {
                    System.err.println("Something is wrong in Table");
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

        for (int i = 0; i < numPage; i++) {
            byte[] temp = ByteBuffer.allocate(4).putInt(pageIDList.get(i)).array();
            result = appendByteBuffer(result, temp);
        }

        // get the total size of table
        int resultSize = result.array().length;
        ByteBuffer temp = ByteBuffer.allocate(4).putInt(resultSize);
        result = appendByteBuffer(temp, result.array());

        return result.array();

    }

    public static ByteBuffer appendByteBuffer(ByteBuffer current, byte[] arr) {
        ByteBuffer result = ByteBuffer.allocate(current.capacity() + arr.length);
        result.put(current.array(), 0, current.array().length);
        result.put(arr, 0, arr.length);
        return result;
    }


    // Given a input string:
    // "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    // check all the possible error and then parse the string to Table Object
    // add the table to the table list
    public Table createTable(String input) {

        String[] dataTypeList = {"integer", "double", "char(", "varchar(", "boolean", "primarykey"};

        //String input = "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)";
        String[] table = input.split("[\\s,]+");

        //remove the "(" after tableName and ")" at end
        table[2] = table[2].split("\\(")[0];
        String substr = table[table.length - 1].substring(table[table.length - 1].length() - 2);

        if (substr.equals(");")) {
            table[table.length - 1] = table[table.length - 1].substring(0, table[table.length - 1].length() - 2);
        }
        List<String> result = new ArrayList<>();
        for (String str : table) {
            if (!str.matches("^[\\s;\\)]*$")) {
                result.add(str);
            }
        }
         table = result.toArray(new String[0]);

        //check if the input array length is a even number or not, if not then error
        if (table.length % 2 != 0) {
            System.err.println("Input's format is wrong! Please check again!");
            System.err.println("ERROR");
            return null;
        }

        //check if there is any primarykey or many primarykey
        int primarykeyNum = 0;
        for (String value : table) {
            if (value.equalsIgnoreCase("primarykey")) {
                primarykeyNum++;
            }
        }
        if (primarykeyNum == 0) {
            System.err.println("No primarykey defined!");
            System.err.println("ERROR");
            return null;
        } else if (primarykeyNum > 1) {
            System.err.println("Can't have multiple primarykey");
            System.err.println("ERROR");
            return null;
        }

        //variables for create new Table: table name, primarykeyName, attriNameList, attriTypeList
        String nameTable = table[2];
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();

        // check if the table already exists
        for (Table tbl : this.tables_list) {
            if (tbl.getTableName().equals(nameTable)) {
                System.err.println("Table of name " + nameTable + " already exists.");
                System.err.println("ERROR");
                return null;
            }
        }

        //loop through the rest of the input
        for (int i = 3; i < table.length; i++) {
            String inStr = table[i];
            //check if inStr is a datatype or not
            boolean isDatatype = false;
            for (String datatype : dataTypeList) {
                if (table[i].contains(datatype)) {
                    isDatatype = true;
                    break;
                }
            }

            //now knowing that this string is not a datatype, check if the next element is a datatype
            if (!isDatatype) {
                boolean containsDatatype = false;
                for (String datatype : dataTypeList) {
                    if (table[i + 1].contains(datatype)) {
                        containsDatatype = true;
                        break;
                    }
                }
                //check if the next string is a datatype or not
                if (!containsDatatype) {
                    System.err.println("This " + table[i + 1] + " is NOT a datatype!");
                    System.err.println("ERROR");
                    return null;
                }

                attriNameList.add(inStr);

            } else {
                //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
                //  enum: {primary 0 1}
                if (inStr.equalsIgnoreCase("primarykey")) {
                    primarykeyName = table[i - 2];
                } else if (inStr.equalsIgnoreCase("boolean")) {
                    attriTypeList.add("20");
                } else if (inStr.equalsIgnoreCase("integer")) {
                    attriTypeList.add("30");
                } else if (inStr.equalsIgnoreCase("double")) {
                    attriTypeList.add("40");
                } else if (inStr.length() > 6 && inStr.length() <= 9) {
                    if (inStr.substring(0, 5).equalsIgnoreCase("char(") &&
                            inStr.substring(inStr.length() - 1).equalsIgnoreCase(")") &&
                            inStr.substring(5, inStr.length() - 1).matches("[0-9]+")) {
                        StringBuilder temp = new StringBuilder();
                        temp.append("5");
                        String typeSize = inStr.substring(5, inStr.length() - 1);
                        temp.append(typeSize);
                        attriTypeList.add(temp.toString());
                    } else {
                        attriNameList.add(inStr);
                    }
                } else if (inStr.length() > 9) {
                    if (inStr.substring(0, 8).equalsIgnoreCase("varchar(") &&
                            inStr.substring(inStr.length() - 1).equalsIgnoreCase(")") &&
                            inStr.substring(8, inStr.length() - 1).matches("[0-9]+")) {
                        StringBuilder temp1 = new StringBuilder();
                        temp1.append("6");
                        String typeSize1 = inStr.substring(8, inStr.length() - 1);
                        temp1.append(typeSize1);
                        attriTypeList.add(temp1.toString());
                    } else {
                        attriNameList.add(inStr);
                    }
                }
            }
        }

        //check if elements in attriNameList are unique, case-insensitive
        for (int i = 0; i < attriNameList.size(); i++) {
            for (int j = i + 1; j < attriNameList.size(); j++) {
                if (attriNameList.get(i).equalsIgnoreCase(attriNameList.get(j))) {
                    System.err.println("ERROR: Attribute names are not unique.");
                    return null;
                }
            }
        }

        //adding table to table list
        Table newTable = new Table(nameTable, primarykeyName, attriNameList, attriTypeList);
        this.tables_list.add(newTable);
        return newTable;
    }

    public void insertTable(Table table){
        tableMap.put(table.getTableName(), table);
    }

}
