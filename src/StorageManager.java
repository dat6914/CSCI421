import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    private String db_loc;
    private int page_size;
    private int buffer_size;
    private Catalog catalog;

    public StorageManager(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
        this.catalog = new Catalog(db_loc);
    }

    /**
     * Method returns the Catalog Object
     * @return catalog
     */
    public Catalog getCatalog() {
        return this.catalog;
    }

    /**
     * Method return the list of tables
     * @return table list
     */
    public ArrayList<Table> getTableList() {
        return this.catalog.getTablesList();
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
                FileOutputStream fs = new FileOutputStream(path, true);
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
                System.out.println("SUCCESS");
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file.");
                e.printStackTrace();
                System.err.println("ERROR");

            }
        }
    }

    /**
     * Method gets record by primary key and table name
     * @param primaryKeyValue primary key of record
     * @param tableName table name
     * @return record
     */
    public Record getRecordByPrimaryKey(String primaryKeyValue, String tableName) {
        Table table = getTableByName(tableName);
        Object primaryKeyObject = convertPrimaryValueToItsType(primaryKeyValue, table);
        String primaryKeyName = table.getPrimaryKeyName();
        int indexOfPrimary = getIndexOfColumn(primaryKeyName, table);
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table, this.db_loc);
            ArrayList<Record> recordArrayList = page.getRecordList();
            for (int j = 0; j < recordArrayList.size(); j++) {
                Record record = recordArrayList.get(j);
                ArrayList<Object> valuesList = record.getValuesList();
                if (valuesList.get(indexOfPrimary).equals(primaryKeyObject)) {
                    return record;
                } else {
                    System.out.println("No record with that primary key");
                    System.out.println("SUCCESS");
                }
            }
        }
        return null;
    }

    /**
     * Method converts primary key value to its type
     * @param primaryKeyValue primary key value
     * @param table table
     * @return Object type of primary key
     */
    public static Object convertPrimaryValueToItsType (String primaryKeyValue, Table table) {
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
     * Method gets the page by pageID of the table by table name
     * @param tableName table name
     * @param pageID page ID
     */
    public boolean getPageByTableAndPageNumber(String tableName, int pageID) {
        Table table = getTableByName(tableName);
        ArrayList<String> attrName = table.getAttriName_list();
        ArrayList<Integer> pageList = table.getPageID_list();
        for (int i = 0; i < pageList.size(); i++) {
            if (i == pageID) {
                Page page = new Page(pageList.get(i), table, this.db_loc);
                ArrayList<Record> recordArrayList = page.getRecordList();
                for (String name : attrName) {
                    System.out.print("\t|\t" + name);
                }
                System.out.println("\n");
                for (Record record : recordArrayList) {
                    System.out.println(printRecord(record));
                }
            } else {
                System.err.println("No pageID in the table.");
                System.err.println("ERROR");
                return false;
            }
        }
        return true;
    }


    /**
     * Method prints all the records of a table
     * @param tableName table name
     */
    public void selectStarFromTable(String tableName) {
        Table table = getTableByName(tableName);
        ArrayList<String> attrName = table.getAttriName_list();
        if (table != null) {
            ArrayList<Record> recordList = getAllRecordsByTable(tableName);
            if (recordList.size() == 0) {
                System.out.println("No record in this table.");
                System.out.println("SUCCESS");
            } else {
                for (String name : attrName) {
                    System.out.print("\t|\t" + name);
                }
                System.out.print("\n");

                for (Record record : recordList) {
                    System.out.println(printRecord(record));
                }
            }
        }
    }


    /**
     * Method get all records in the table by table name
     * @param tableName table name
     * @return arraylist of records of the table
     */
    public ArrayList<Record> getAllRecordsByTable(String tableName) {
        ArrayList<Record> result = new ArrayList<>();
        Table table = getTableByName(tableName);
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table, this.db_loc);
            ArrayList<Record> recordArrayList = page.getRecordList();
            result.addAll(recordArrayList);
        }
        if (result.size() == 0) {
            return null;
        }
        return result;
    }


    /**
     * Method prints out the Record
     * @param record record needs to be printed out
     * @return String of record
     */
    public static String printRecord(Record record) {
        StringBuilder stringBuilder = new StringBuilder();
        ArrayList<Object> valuesList = record.getValuesList();
        for (Object value : valuesList) {
            stringBuilder.append(" \t|\t");
            stringBuilder.append(value);
            stringBuilder.append(" \t|\t");
        }
        return stringBuilder.toString();
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
     * @return Table
     */
    public Table getTableByName(String tableName) {
        ArrayList<Table> tableArrayList = getTableList();
        for (Table table : tableArrayList) {
            if (table.getTableName().equalsIgnoreCase(tableName)) {
                return table;
            } else {
                System.err.println("No such table " + tableName);
                return null;
            }
        }
        System.err.println("No such table " + tableName);
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


    //TODO
    public boolean deleteRecord(String primaryKeyValue, String tableName) {
        Table table = getTableByName(tableName);
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (Integer page : pageIDlist) {
            //Open the page and call function convert the page to readable string and print
            //error if can't read the page

        }

        return false;
    }


    //TODO
    public boolean updateRecord(String tableName, String primaryKeyValue, String inputRecordUpdate) {
        //error if there is no record with primarykeyValue
        Record record = getRecordByPrimaryKey(primaryKeyValue, tableName);
        //check the inputRecord
        //if pass the check then call deleteRecord and insertRecord

        return false;
    }


//    public TreeMap<Integer, ArrayList<Record>> getMapPageIDRecord(Table table) {
//        TreeMap<Integer, ArrayList<Record>> result = new TreeMap<>();
//        ArrayList<Integer> pageIDList = table.getPageID_list();
//        for (Integer pageID : pageIDList) {
//            ArrayList<Record> recordArrayList = (new Page(pageID, table, this.db_loc)).getRecordList();
//            result.put(pageID, recordArrayList);
//        }
//        return result;
//    }

//    //Open every Page and convert the page to Arraylist of Record
//    //then add the primaryvalue to the arraylist<Object> by calling function find index of attributeName: getIndexOfColumn(primaryKeyName)
//    // then check if the insert input has unique value and non-null
//    public ArrayList<Object> getPrimarykeyValueList(Table table) {
//        ArrayList<Object> result = new ArrayList<>();
//        ArrayList<Integer> pageIDlist = table.getPageID_list();
//        for (int j = 0; j < pageIDlist.size(); j++) {
//            int pageID = pageIDlist.get(j);
//            Page page = new Page(pageID, table, this.db_loc);
//            ArrayList<Record> record_list = page.getRecordList();
//            for (int i = 0; i < record_list.size(); i++) {
//                Record record = record_list.get(i);
//                String primaryKey = table.getPrimaryKeyName();
//                int indexOfPrimaryKey = getIndexOfColumn(primaryKey, table);
//                result.add(record.getValuesList().get(indexOfPrimaryKey));
//            }
//        }
//        return result;
//    }

//    public void sortPrimaryKey(ArrayList<Object> primarykeyValueList) {
//        Collections.sort(primarykeyValueList, comparator);
//    }


//    /**
//     * Method removes element of String array
//     *
//     * @param old   the current String array
//     * @param index index of element that will be removed
//     * @return String array after remove
//     */
//    private String[] removeElementInStringArray(String[] old, int index) {
//        String[] newArray = new String[old.length - 1];
//        for (int i = 0, j = 0; i < old.length; i++) {
//            if (i != index) {
//                newArray[j++] = old[i];
//            }
//        }
//        return newArray;
//    }
//
//
//    // Define a comparator to compare the objects in the list
//    Comparator<Object> comparator = new Comparator<Object>() {
//        public int compare(Object o1, Object o2) {
//            if (o1 instanceof Integer && o2 instanceof Integer) {
//                return ((Integer) o1).compareTo((Integer) o2);
//            } else if (o1 instanceof Double && o2 instanceof Double) {
//                return ((Double) o1).compareTo((Double) o2);
//            } else if (o1 instanceof String && o2 instanceof String) {
//                return ((String) o1).compareTo((String) o2);
//            } else if (o1 instanceof Boolean && o2 instanceof Boolean) {
//                return ((Boolean) o1).compareTo((Boolean) o2);
//            } else {
//                // Objects of different types should be considered equal
//                return 0;
//            }
//        }
//    };

}
