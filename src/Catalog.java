
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class Catalog {

    ArrayList<Table> tables_list = new ArrayList<>();



    public Catalog() {

    }

    /*
     * Most of these functions are gonna be used by the query processor and since this is the catalog class,
     * make sure we're not touching ANY DATA HERE. Data has to be stored in binary on hardware and must be saved between runs.
     */


    public void displayTableInfo(String tableName) {
        for (Table table : tables_list) {
            if (table.getTableName().equals(tableName)) {
                System.out.println("Table name: " + table.getTableName());
                System.out.println("Table schema: " + table.getSchema());
                System.out.println("Pages: " + table.getPage_list().size());
                System.out.println("Records: " + table.getRecord_list().size());

                System.out.println("\nSUCCESS\n");
                return;
            }
        }
        System.err.println("ERROR: Table does not exist.");
    }

    //TODO: when the command <quit> then write table to schema and then add to tablelist
    //for now i just write directly for TESTING
    //Think of how to store the number of tables in schema, and append table bytearray to file
    public void saveTableToCatalogAndDisk(String path, byte[] data) {
        File tempFile = new File(path);
        if (tempFile.exists()) {
            try (FileWriter f = new FileWriter(path, true);
                    BufferedWriter b = new BufferedWriter(f);
                    PrintWriter p = new PrintWriter(b)) {

                p.println(data);

            } catch (IOException i) {
                System.err.println("An error occurred while writing to the file.");
                i.printStackTrace();
                System.err.println("ERROR");
            }

        } else {
            try {
                FileOutputStream fs = new FileOutputStream(path);
                fs.write(data);
                System.out.println("SUCCESS");
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file.");
                e.printStackTrace();
                System.err.println("ERROR");
            }
        }
    }

    public byte[] readByteArrFromCatalogFile() {
        return null;
    }

    // Given a input string:
    //  "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    // check all the possible error and then parse the string to Table Object
    // I did NOT add the table to the table list yet //TODO will add when the user enter command <quit>
    public Table createTable (String input) {

        String[] dataTypeList = {"integer", "double", "char(", "varchar(", "boolean", "primarykey"};

        //String input = "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)";
        String[] table = input.split("[\\s,]+");

        //remove the "(" after tableName and ")" at end
        table[2] = table[2].split("\\(")[0];
        table[table.length-1] = table[table.length-1].split("\\)")[0];

        //check if the input array length is a even number or not, if not then error
        if (table.length % 2 != 0) {
            System.err.println("Input's format is wrong! Please check again!");
            System.err.println("ERROR");
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
        } else if (primarykeyNum > 1) {
            System.err.println("Can't have multiple primarykey");
            System.err.println("ERROR");
        }

        //variables for create new Table: table name, primarykeyName, attriNameList, attriTypeList
        String nameTable = table[2];
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();

        // check if the table already exists
        for (Table tbl : tables_list) {
            if (tbl.getTableName().equals(nameTable)) {
                System.err.println("ERROR: Table already exists.");
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
                    if (table[i+1].contains(datatype)) {
                        containsDatatype = true;
                        break;
                    }
                }
                //check if the next string is a datatype or not
                if (!containsDatatype) {
                    System.err.println("This " + table[i+1] + " is NOT a datatype!");
                    System.err.println("ERROR");
                }

                attriNameList.add(inStr);

            } else {
                //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
                //  enum: {primary 0 1}
                if (inStr.equalsIgnoreCase("primarykey")){
                    primarykeyName = table[i-2];
                } else if (inStr.equalsIgnoreCase("boolean")) {
                    attriTypeList.add("20");
                } else if (inStr.equalsIgnoreCase("integer")) {
                    attriTypeList.add("30");
                } else if (inStr.equalsIgnoreCase("double")) {
                    attriTypeList.add("40");
                } else if (inStr.length()>6 && inStr.length() <= 9) {
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
                }
            }
        }

        //adding table to table list
        Table newTable = new Table(nameTable, primarykeyName, attriNameList, attriTypeList);

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

    public byte[] convertTableToByteArr (Table table) {

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
            result = appendByteBuffer(result,attriTypeArr);

            int tempForTypeSize = 0;
            if (attriType == 2 || attriType == 3 || attriType == 4) {
                tempForTypeSize = 0;
            } else {
                String typeSize = table.getAttrType(i).substring(1);
                try{
                    tempForTypeSize = Integer.parseInt(typeSize);
                }
                catch (NumberFormatException ex){
                    System.err.println("Something is wrong in Table");
                    ex.printStackTrace();
                    System.err.println("ERROR");
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

        // endcode the total size of table
        int resultSize = result.array().length;
        ByteBuffer temp = ByteBuffer.allocate(4).putInt(resultSize);
        result = appendByteBuffer(temp, result.array());

        return result.array();

    }

    private ByteBuffer appendByteBuffer(ByteBuffer current, byte[] arr) {
        ByteBuffer result = ByteBuffer.allocate(current.capacity() + arr.length);
        result.put(current.array(), 0, current.array().length);
        result.put(arr, 0, arr.length);
        return result;
    }

    //FORMAT: student name varchar(15) studentID integer primarykey address char(20) gpa double incampus boolean
    public Table convertByteArrToTable(byte[] tableArr) {

        ByteBuffer result = ByteBuffer.wrap(tableArr);
        // get 4 bytes of tableName size
        int tableNameSize = result.getInt(4);
        byte[] tableNameArr = Arrays.copyOfRange(tableArr, 8, tableNameSize+8);

        //Variables need for create a table
        String tableName = new String(tableNameArr, StandardCharsets.UTF_8);
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();

        // get next 4 bytes for the number of attributes
        byte[] numberAttributes = Arrays.copyOfRange(tableArr, 8+tableNameSize, 8+tableNameSize+4);
        int attributeNum = ByteBuffer.wrap(numberAttributes).getInt();

        // get all the attributes
        int indexTracking = 8+tableNameSize+4;
        for (int i = 0; i < attributeNum; i++) {
            // get next 4 bytes for the attribute name size
            byte[] attriNameSizeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+4);
            int attriNameSize = ByteBuffer.wrap(attriNameSizeArr).getInt();
            indexTracking = indexTracking+4;

            // get the next attriNameSize for the name of attribute and add to the attribute name list
            byte[] attriNameArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+attriNameSize);
            indexTracking = indexTracking + attriNameSize;
            String attrName = new String(attriNameArr, StandardCharsets.UTF_8);
            attriNameList.add(attrName);

            // now for attribute type
            StringBuilder attriTypeStrBuilder = new StringBuilder();
            // get next 4 bytes for attribute type. It will follow the hard set up:
            //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6}
            byte[] attriTypeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+4);
            int attriType = ByteBuffer.wrap(attriTypeArr).getInt();
            indexTracking = indexTracking+4;
            attriTypeStrBuilder.append(attriType);

            // get next 4 bytes for attribute type size.
            // by setup, 0 if integer, boolean, or double, and the size for varchar and char
            byte[] attriTypeSizeArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+4);
            int attriTypeSize = ByteBuffer.wrap(attriTypeSizeArr).getInt();
            indexTracking = indexTracking+4;
            attriTypeStrBuilder.append(attriTypeSize);

            // get next 4 bytes for primary key by hard setup:
            //  enum: {primary 0 1}
            byte[] primarykey = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+4);
            int primaryNum = ByteBuffer.wrap(primarykey).getInt();
            indexTracking = indexTracking+4;
            if (primaryNum == 1) {
                primarykeyName = attriNameList.get(attriNameList.size()-1);
            }

            // only add the attribute type (first digit) and the size of attribute type (from the 2nd digit)
            attriTypeList.add(attriTypeStrBuilder.toString());

        }

        if (attriNameList.size() != attriTypeList.size()) {
            System.err.println("Something goes wrong. Can't read the catalog file.");
            System.err.println("ERROR");
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
        }

        Table newTable = new Table(tableName, primarykeyName, attriNameList, attriTypeList);
        return newTable;
    }



}
