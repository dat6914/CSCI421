
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
                    PrintWriter p = new PrintWriter(b);) {

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

    //Given a input string:
    //  "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    //check all the possible error and then parse the string to Table Object
    //I did NOT add the table to the table list yet
    public Table createTable (String input) throws IOException {

        String[] dataTypeList = {"integer", "double", "char", "varchar", "boolean", "primarykey"};

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
                if (inStr.equalsIgnoreCase("primarykey")){
                    primarykeyName = table[i-2];
                } else {
                    attriTypeList.add(inStr);
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

    //FORMAT: student name varchar(15) studentID integer primarykey address char(20) gpa double incampus boolean
    // Byte[]:  first 4 bytes for the size of whole string HERE: 139
    //          next 4 bytes store the size of tableName HERE: 7
    //          next 7 bytes store "student"
    //          next 4 bytes store the number of attributes HERE: 5
    //          next 4 bytes store the size of name of 1st attribute HERE: 4
    //          next 4 bytes store "name"
    //          next 4 bytes store the size attri type of 1st attr HERE: 11
    //          next 11 bytes store "varchar(15)"
    //          next 4 bytes store the size of name of 2nd attr HERE: 9
    //          next 9 bytes store "studentID"
    //          ....
    //  Total size of this table is : 139 + 4 = 143
    public byte[] convertTableToByteArr (Table table) {

        ByteBuffer result = ByteBuffer.allocate(0);
        int tableNameSize = table.getTableName().length();
        byte[] tempNameSize = ByteBuffer.allocate(4).putInt(tableNameSize).array();
        byte[] tableNameArr = table.getTableName().getBytes(StandardCharsets.UTF_8);

        result = appendByteBuffer(result, tempNameSize);
        result = appendByteBuffer(result, tableNameArr);

        int numberAttriName = table.getAttriName_list().size();
        byte[] numAttriName = ByteBuffer.allocate(4).putInt(numberAttriName).array();
        result = appendByteBuffer(result, numAttriName);

        for (int i = 0; i < table.getAttriName_list().size(); i++) {

            int attriNameSize = table.getAttriName_list().get(i).length();
            byte[] attriNameSizeArr = ByteBuffer.allocate(4).putInt(attriNameSize).array();
            byte[] attriNameArr = table.getAttriName_list().get(i).getBytes(StandardCharsets.UTF_8);
            result = appendByteBuffer(result, attriNameSizeArr);
            result = appendByteBuffer(result, attriNameArr);

            int attriTypeSize = table.getAttriType_list().get(i).length();
            byte[] attriTypeSizeArr = ByteBuffer.allocate(4).putInt(attriTypeSize).array();
            byte[] attriTypeArr = table.getAttriType_list().get(i).getBytes(StandardCharsets.UTF_8);
            result = appendByteBuffer(result, attriTypeSizeArr);
            result = appendByteBuffer(result, attriTypeArr);

            if (table.getPrimaryKeyName().equals(table.getAttriName_list().get(i))) {
                int primarykeySize = "primarykey".length();
                byte[] primarkeySizeArr = ByteBuffer.allocate(4).putInt(primarykeySize).array();
                byte[] primarykeyArr = "primarykey".getBytes(StandardCharsets.UTF_8);
                result = appendByteBuffer(result, primarkeySizeArr);
                result = appendByteBuffer(result, primarykeyArr);
            }
        }

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
        int tableSize = result.getInt(0);
        int tableNameSize = result.getInt(4);
        byte[] tableNameArr = Arrays.copyOfRange(tableArr, 8, tableNameSize+8);

        //Variables need for create a table
        String tableName = new String(tableNameArr, StandardCharsets.UTF_8);
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();

        byte[] numberAttributes = Arrays.copyOfRange(tableArr, 8+tableNameSize, 8+tableNameSize+4);
        int attributeNum = ByteBuffer.wrap(numberAttributes).getInt();

        int indexTracking = 8+tableNameSize+4;
        for (int i = 0; i < attributeNum*2+1; i++) {
            byte[] attriArr = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+4);
            int attriSize = ByteBuffer.wrap(attriArr).getInt();
            indexTracking = indexTracking+4;
            byte[] attriName = Arrays.copyOfRange(tableArr, indexTracking, indexTracking+attriSize);
            indexTracking = indexTracking + attriSize;
            String temp = new String(attriName, StandardCharsets.UTF_8);
            if (temp.equalsIgnoreCase("primarykey")) {
                String primarykey = attriNameList.get(attriNameList.size()-1);
                primarykeyName = primarykey;
            } else if (temp.equalsIgnoreCase("integer")) {
                attriTypeList.add("integer");
            } else if (temp.equalsIgnoreCase("double")) {
                attriTypeList.add("double");
            } else if (temp.equalsIgnoreCase("boolean")) {
                attriTypeList.add("boolean");
            } else if (temp.length()>6 && temp.length() <= 9) {
                if (temp.substring(0, 5).equalsIgnoreCase("char(") &&
                        temp.substring(temp.length() - 1).equalsIgnoreCase(")") &&
                        temp.substring(5, temp.length() - 1).matches("[0-9]+")) {
                    attriTypeList.add(temp);
                } else {
                    attriNameList.add(temp);
                }
            } else if (temp.length() > 9) {
                if (temp.substring(0,8).equalsIgnoreCase("varchar(") &&
                        temp.substring(temp.length()-1).equalsIgnoreCase(")") &&
                        temp.substring(8, temp.length()-1).matches("[0-9]+")) {
                    attriTypeList.add(temp);
                } else {
                    attriNameList.add(temp);
                }
            } else {
                attriNameList.add(temp);
            }
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
