package Main;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    private String db_loc;
    private int page_size;
    private int buffer_size;
    private Catalog catalog;

    private Catalog tempCatalog;

    public StorageManager(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
        this.catalog = new Catalog(db_loc);
        this.tempCatalog = this.catalog;
    }

    /**
     * Method returns the Main.Catalog Object
     * @return catalog
     */
    public Catalog getCatalog() {
        return this.catalog;
    }

    public Catalog getTempCatalog() {
        return this.tempCatalog;
    }

    public void setTempCatalog(Catalog catalog) {
        this.tempCatalog = catalog;
    }


    // Given a input string:
    // "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean);"
    // check all the possible error and then parse the string to Main.Table Object
    // add the table to the table list

    /**
     * Method is called when create table SQL command
     * @param input create table SQL input
     * @return table
     */
    public Table createTable(String input) {

        //String input = "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)";
        String[] table = input.split("[\\s,]+");

        //remove the "(" after tableName
        char lastChar = table[2].charAt(table[2].length()-1);
        if (lastChar == '(') {
            table[2] = table[2].split("\\(")[0];
        } else if (table[3].equalsIgnoreCase("(")) {
            table = removeElementInStringArray(table, 3);
        }

        //remove the "(" before the first attribute name
        char firstChar = table[3].charAt(0);
        if (firstChar == '(') {
            table[3] = table[3].substring(1);
        }

        //remove the ");" at end
        String substr = table[table.length - 1].substring(table[table.length - 1].length() - 2);
        if (substr.equals(");")) {
            table[table.length - 1] = table[table.length - 1].substring(0, table[table.length - 1].length() - 2);
        } else {
            table = removeElementInStringArray(table, table.length - 1);
        }

        List<String> result = new ArrayList<>();
        for (String str : table) {
            if (!str.matches("^[\\s;)]*$")) {
                result.add(str);
            }
        }
        table = result.toArray(new String[0]);

        //check if there is any primarykey or many primarykey
        int primarykeyNum = 0;
        int indexOfprimaryKey = 0;
        for (int i = 0; i < table.length; i++) {
            if (table[i].equalsIgnoreCase("primarykey")) {
                primarykeyNum++;
                indexOfprimaryKey = i;
            }
        }
        if (primarykeyNum == 0) {
            System.err.println("No primary key defined!");
            System.err.println("ERROR");
            return null;
        } else if (primarykeyNum > 1) {
            System.err.println("More than one primary key");
            System.err.println("ERROR");
            return null;
        }

        //variables for create new Main.Table: table name, primarykeyName, attriNameList, attriTypeList
        String nameTable = table[2];
        String primarykeyName = "";
        ArrayList<String> attriNameList = new ArrayList<>();
        ArrayList<String> attriTypeList = new ArrayList<>();

        // check if the table already exists
        for (Table tbl : this.catalog.getTablesList()) {
            if (tbl.getTableName().equals(nameTable)) {
                System.err.println("Table of name " + nameTable + " already exists.");
                System.err.println("ERROR");
                return null;
            }
        }

        //System.out.println(table);
        //loop through the rest of the input
        for (int i = 3; i < table.length; i++) {
            String inStr = table[i];
            //System.out.println("inStr: " + inStr);
            //index of datatype before "primarykey" is even
            if (i < indexOfprimaryKey) {
                if (i%2 == 0) {
                    if (!(inStr.equals("integer") || inStr.equals("double") ||inStr.equals("boolean")
                            || inStr.startsWith("varchar(") || inStr.startsWith("char("))) {
                        System.err.println("This " + inStr + " is NOT a datatype!");
                        System.err.println("ERROR");
                        return null;
                    }
                    parseAttributeTypeToNumber(attriTypeList, inStr);
                } else {
                    attriNameList.add(inStr);
                }
            } else if (i == indexOfprimaryKey) {
                primarykeyName = table[i-2];
            } else {
                if (i%2 != 0) {
                    if (!(inStr.equals("integer") || inStr.equals("double") ||inStr.equals("boolean")
                            || inStr.startsWith("char(") || inStr.startsWith("varchar("))) {
                        System.err.println("This " + inStr + " is NOT a datatype!");
                        System.err.println("ERROR");
                        return null;
                    }
                    parseAttributeTypeToNumber(attriTypeList, inStr);
                } else {
                    attriNameList.add(inStr);
                }
            }

        }

        //check if elements in attriNameList are unique, case-insensitive
        for (int i = 0; i < attriNameList.size(); i++) {
            for (int j = i + 1; j < attriNameList.size(); j++) {
                if (attriNameList.get(i).equalsIgnoreCase(attriNameList.get(j))) {
                    System.err.println("Duplicate attribute name \"" + attriNameList.get(i) + "\"");
                    System.err.println("ERROR");
                    return null;
                }
            }
        }

        if (attriNameList.size() != attriTypeList.size()) {
            System.err.println("Number of attribute names is NOT equal to number of attribute types!");
            System.err.println("ERROR");
            return null;
        }

        //adding table to table list
        ArrayList<Integer> pageList = new ArrayList<>();
        Table newTable = new Table(nameTable, primarykeyName, attriNameList, attriTypeList, this.db_loc, pageList);
        this.catalog.getTablesList().add(newTable);
        return newTable;
    }


    /**
     * Method parses attribute type to a number
     * @param attriTypeList arraylist of attribute types
     * @param inStr string input
     */
    private void parseAttributeTypeToNumber(ArrayList<String> attriTypeList, String inStr) {
        if (inStr.equalsIgnoreCase("boolean")) {
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
            }
        }
    }


    /**
     * Method writes the byte array of object to file
     * @param path the path of file
     * @param data byte array of data need to be stored in file
     */
    public void writeByteArrToDisk(String path, byte[] data) {
        File tempFile = new File(path);
        if (tempFile.exists()) {
            try {
                FileOutputStream fs = new FileOutputStream(path);
                fs.write(data);
                fs.close();
            } catch (IOException i) {
                System.err.println("An error occurred while writing to the file.");
                i.printStackTrace();
                System.err.println("ERROR");
            }

        } else {
            try {
                FileOutputStream fs = new FileOutputStream(path);
                fs.write(data);
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file.");
                e.printStackTrace();
                System.err.println("ERROR");

            }
        }
    }


    /**
     * Method converts primary key value to its type
     * @param primaryKeyValue primary key value
     * @param table table
     * @return Object type of primary key
     */
    public Object convertPrimaryValueToItsType (String primaryKeyValue, Table table) {
        ArrayList<String> attributeTypeList = table.getAttriType_list();
        int indexPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
        for (int i = 0; i < attributeTypeList.size(); i++) {
            char primaryType = attributeTypeList.get(indexPrimaryKey).charAt(0);
            if (primaryType == '3') {
                if (isInteger(primaryKeyValue)) {
                    return Integer.parseInt(primaryKeyValue);
                } else {
                    System.err.println("ERROR");
                    return null;
                }
            } else if (primaryType == '4') {
                if (isDouble(primaryKeyValue)) {
                    return Double.parseDouble(primaryKeyValue);
                } else {
                    System.err.println("ERROR");
                    return null;
                }
            } else if (primaryType == '2') {
                if (isBoolean(primaryKeyValue)) {
                    return primaryKeyValue.equalsIgnoreCase("true") ? Boolean.TRUE :
                            primaryKeyValue.equalsIgnoreCase("false") ? Boolean.FALSE : null;
                } else {
                    System.err.println("ERROR");
                    return null;
                }
            } else {
                return primaryKeyValue;
            }
        }
        System.err.println("ERROR");
        return null;
    }


    /**
     * This method checks if a string is an boolean or not
     * @param str string needs to be checks
     * @return true if a string is boolean, false if not
     */
    private static boolean isBoolean(String str) {
        return str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false");
    }


    /**
     * This method checks if a string is an double or not
     * @param str string needs to be checks
     * @return true if a string is double, false if not
     */
    private static boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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
     * Method get table by table name
     * @param tableName table name
     * @return Main.Table
     */
    public Table getTableByName(String tableName) {
        ArrayList<Table> tableArrayList = this.catalog.getTablesList();
        for (Table table : tableArrayList) {
            if (table.getTableName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        System.err.println("No such table " + tableName);
        System.err.println("ERROR");
        return null;
    }


    /**
     * Method gets the index of attribute name in the attribute name list
     * @param attrName attribute name
     * @return index of attribute name
     */
    private static int getIndexOfColumn(String attrName, Table table) {
        ArrayList<String> attriName_list = table.getAttriName_list();
        int index = -1;
        for (int i = 0; i < attriName_list.size(); i++) {
            if (attriName_list.get(i).equals(attrName)) {
                index = i;
            }
        }
        return index;
    }


    /**
     * Method removes element at index in a string array
     * @param old string array need to be removed an element
     * @param index index of element want to remove
     * @return string array after removing element
     */
    public String[] removeElementInStringArray(String[] old, int index) {
        String[] newArray = new String[old.length - 1];
        for (int i = 0, j = 0; i < old.length; i++) {
            if (i != index) {
                newArray[j++] = old[i];
            }
        }
        return newArray;
    }

    //TODO
    public boolean deleteRecord(String primaryKeyValue, String tableName) {

        return false;
    }


    //TODO
    public boolean updateRecord(String tableName, String primaryKeyValue, String inputRecordUpdate) {

        return false;
    }
}
