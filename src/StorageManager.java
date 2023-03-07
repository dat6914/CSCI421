import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class StorageManager {

    private PageBuffer pageBuffer;

    private Catalog catalog;

    private String db_loc;

    private int pageSize;

    ArrayList<Page> pageList = new ArrayList<>();


    public StorageManager(String db_loc, int pageSize, int bufferSize){
        this.db_loc = db_loc;
        this.pageSize = pageSize;
        this.catalog = new Catalog(db_loc);
        this.pageBuffer = new PageBuffer(db_loc, bufferSize, pageList);

    }


    public Catalog getCatalog(){
        return this.catalog;
    }

    public PageBuffer getPageBuffer(){
        return  this.pageBuffer;
    }


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

        //variables for create new Table: table name, primarykeyName, attriNameList, attriTypeList
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

        System.out.println(table);
        //loop through the rest of the input
        for (int i = 3; i < table.length; i++) {
            String inStr = table[i];
            System.out.println("inStr: " + inStr);
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

    public String[] removeElementInStringArray(String[] old, int index) {
        String[] newArray = new String[old.length - 1];
        for (int i = 0, j = 0; i < old.length; i++) {
            if (i != index) {
                newArray[j++] = old[i];
            }
        }
        return newArray;
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
     * Method get table by table name
     * @param tableName table name
     * @return Table
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
     * Method removes the first and last of a string
     * @param input string
     * @return string after removing
     */
    private static String removeFirstAndLastChar(String input) {
        if (input != null && input.length() > 2) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }

    /**
     * Method converts byte array of record to record object
     * @param strArr byte array of record
     * @param table table
     * @return record object
     */
    public Record convertStringArrToRecord(String[] strArr, Table table) {
        ArrayList<Pointer> pointerholder = new ArrayList<>();
        ArrayList<Object> valuesList = new ArrayList<>();
        String primaryKey = table.getPrimaryKeyName();
        ArrayList<String> attriTypeList = table.getAttriType_list();

        //check primaryKey if duplication and can't be null
        int indexOfPrimaryKey = getIndexOfColumn(primaryKey, table);
        for (int i = 0; i < strArr.length; i++) {
            char attrType = attriTypeList.get(i).charAt(0);
            System.out.println(attrType);
            if (i == indexOfPrimaryKey) {
                String value = strArr[i];
                if (value.equalsIgnoreCase("null")) {
                    System.err.println("Primary key can't be null");
                    System.err.println("ERROR");
                    return null;
                }
            }
            if (attrType == '2') {
                if (isBoolean(strArr[i])) {
                    Object valueObj = strArr[i].equalsIgnoreCase("true") ? Boolean.TRUE :
                            strArr[i].equalsIgnoreCase("false") ? Boolean.FALSE : null;
                    valuesList.add(valueObj);
                } else {
                    System.err.println("Expect " + strArr[i] + " is a boolean: true/false but got " + strArr[i]);
                    System.err.println("ERROR");
                    return null;
                }
            } else if (attrType == '3') {
                if (isInteger(strArr[i])) {
                    Object valueObj = Integer.valueOf(strArr[i]);
                    valuesList.add(valueObj);
                } else {
                    System.err.println("Expect " + strArr[i] + " is a integer but got " + strArr[i]);
                    System.err.println("ERROR");
                    return null;
                }
            } else if (attrType == '4') {
                if (isDouble(strArr[i])) {
                    Object valueObj = Double.valueOf(strArr[i]);
                    valuesList.add(valueObj);
                } else {
                    System.err.println("Expect " + strArr[i] + " is a double but got " + strArr[i]);
                    System.err.println("ERROR");
                    return null;
                }
            } else if (attrType == '5') {
                if(!strArr[i].substring(0,1).equals("\"") || !strArr[i].substring(strArr[i].length()-1, strArr[i].length()).equals("\"")){
                    System.err.println("Expect quotations for string values");
                    System.err.println("ERROR");
                    return null;
                }
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);

                String sizeString = attriTypeList.get(i).substring(1);
                if (isInteger(sizeString)) {
                    int size = Integer.parseInt(sizeString);
                    int inputSize = strArr[i].length();
                    if (inputSize != size) {
                        System.err.println("Expect " + strArr[i] + " has " + size + " chars but got " + inputSize + " chars");
                        System.err.println("ERROR");
                        return null;
                    } else {
                        valuesList.add(strArr[i]);
                    }
                }
            } else if (attrType == '6') {
                if(!strArr[i].substring(0,1).equals("\"") || !strArr[i].substring(strArr[i].length()-1, strArr[i].length()).equals("\"")){
                    System.err.println("Expect quotations for string values");
                    System.err.println("ERROR");
                    return null;
                }
                // take out the quote in the front and at the end.
                strArr[i] = strArr[i].substring(1, strArr[i].length()-1);

                String sizeString = attriTypeList.get(i).substring(1);
                if (isInteger(sizeString)) {
                    int size = Integer.parseInt(sizeString);
                    int inputSize = strArr[i].length();
                    if (inputSize > size) {
                        System.err.println("Expect " + strArr[i] + " has " + size + " chars but got " + inputSize + " chars");
                        System.err.println("ERROR");
                        return null;
                    } else {
                        valuesList.add(strArr[i]);
                    }
                }
            } else {
                System.err.println("Fail to parse the input to record");
                System.err.println("ERROR");
                return null;
            }
        }
        return new Record(valuesList,pointerholder);
    }

    /**
     * Method checks if a string is boolean
     * @param str string
     * @return true if a boolean otherwise false
     */
    private static boolean isBoolean(String str) {
        return str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false");
    }


    /**
     * Method checks if a string is double
     * @param str string
     * @return true if a double otherwise false
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

    // "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    // FORMAT: insert into student values ("Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true),("(A)" 1 "school" 2 "(false")
    // check: many tuples in 1 sql, check duplicate primary, check how many attributes, check the type of attribute,
    // check the length in varchar and char, check null, check if table name is exist

    /**
     * Method return the input record string to the list of string array
     * @param inputTupesList record string input
     * @return arraylist of array string (one string[] is the record string)
     */
    public ArrayList<Record> returnListofStringArrRecord(String inputTupesList, Table table) {
        ArrayList<Record> recordList = new ArrayList<>();
        String[] splitString = inputTupesList.split(",");

        for (String str : splitString) {
            if(!str.substring(0,1).equals("(") || !str.substring(str.length()-1, str.length()).equals(")")){
                return null;
            }
            String tupleAfterRemoveParenthesis = removeFirstAndLastChar(str);
            String[] tupleSplitBySpace = tupleAfterRemoveParenthesis.split(" ");
            if(tupleSplitBySpace.length!=table.getAttriName_list().size()){
                // number of attributes don't match
                return null;
            }

            Record r = convertStringArrToRecord(tupleSplitBySpace, table);
            if (r != null){
                recordList.add(r);
            }
        }
        return recordList;
    }

    /**
     * Method compares 2 Objects
     * @param o1 object 1
     * @param o2 object 2
     * @return 0 is equals, -1 if ob1 < ob2, and 1 if ob1 > ob2
     */
    public int compare2Records(Object o1, Object o2) {
        if (o1 instanceof Integer && o2 instanceof Integer) {
            return ((Integer) o1).compareTo((Integer) o2);
        } else if (o1 instanceof Double && o2 instanceof Double) {
            return ((Double) o1).compareTo((Double) o2);
        } else if (o1 instanceof String && o2 instanceof String) {
            return ((String) o1).compareTo((String) o2);
        } else if (o1 instanceof Boolean && o2 instanceof Boolean) {
            return ((Boolean) o1).compareTo((Boolean) o2);
        } else {
            // Objects of different types should be considered equal
            return 0;
        }
    }

    /**
     * Method return string of record by primaryKey
     * @param record record
     * @return string of record
     */
    public String getRecordByPrimary(Record record) {
        return printRecord(record);
    }


    /**
     * Method gets the page by pageID of the table by table name
     * @param table table
     * @param pageNum pageNum
     */
    public boolean getPageByTableAndPageNumber(Table table, int pageNum) {
        ArrayList<String> attrName = table.getAttriName_list();
        ArrayList<Integer> pageList = table.getPageID_list();
        ArrayList<Pointer> pointerholder = new ArrayList<>();
        for (int i = 0; i < pageList.size(); i++) {
            if (i == pageNum - 1) {
                Page page = new Page(pageList.get(i), table, this.db_loc,pointerholder);
                if (pageBuffer.getPageList().contains(page)) {
                    for (Page tempPage : pageBuffer.getPageList()) {
                        if (tempPage.equals(page)) {
                            page = tempPage;
                            break;
                        }
                    }
                } else {
                    pageBuffer.getPageList().add(page);
                }
                ArrayList<Record> recordArrayList = page.getRecordList();
                for (String name : attrName) {
                    System.out.print("  |  " + name + "  |  ");
                }
                System.out.print("\n");
                for (Record record : recordArrayList) {
                    System.out.println(printRecord(record));
                }
            }
        }
        return true;
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
            stringBuilder.append("  |  ");
            stringBuilder.append(value);
            stringBuilder.append("  |  ");
        }
        return stringBuilder.toString();
    }

    /**
     * Method gets the list of pageID
     * @return arraylist of pageID
     */
    public ArrayList<Integer> getPageIDList() {
        ArrayList<Integer> result = new ArrayList<>();
        ArrayList<Table> tableList = this.catalog.getTablesList();
        for (Table table : tableList) {
            result.addAll(table.getPageID_list());
        }
        return result;
    }

    /**
     * Method get all records in the table by table name
     * @param table table
     * @return arraylist of records of the table
     */
    public ArrayList<Record> getAllRecordsByTable(Table table) {
        ArrayList<Record> result = new ArrayList<>();
        ArrayList<Pointer> pointerholder = new ArrayList<>();
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table, this.db_loc,pointerholder);
            if (pageBuffer.getPageList().contains(page)) {
                for (Page tempPage : pageBuffer.getPageList()) {
                    if (tempPage.equals(page)) {
                        page = tempPage;
                        break;
                    }
                }
            } else {
                this.pageBuffer.getPageList().add(page);
            }
            ArrayList<Record> recordArrayList = page.getRecordList();
            result.addAll(recordArrayList);
        }
        if (result.size() == 0) {
            return null;
        }
        return result;
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
     * Method gets record by primary key and table name
     * @param primaryKeyValue primary key of record
     * @param table table
     * @return record
     */
    public Record getRecordByPrimaryKey(String primaryKeyValue, Table table) {
        Object primaryKeyObject = convertPrimaryValueToItsType(primaryKeyValue, table);
        String primaryKeyName = table.getPrimaryKeyName();
        int indexOfPrimary = getIndexOfColumn(primaryKeyName, table);
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        ArrayList<Pointer> pointerholder = new ArrayList<>();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table, this.db_loc, pointerholder);
            if (pageBuffer.getPageList().contains(page)) {
                for (Page tempPage : pageBuffer.getPageList()) {
                    if (tempPage.equals(page)) {
                        page = tempPage;
                        break;
                    }
                }
            } else {
                this.pageBuffer.getPageList().add(page);
            }
            ArrayList<Record> recordArrayList = page.getRecordList();
            for (int j = 0; j < recordArrayList.size(); j++) {
                Record record = recordArrayList.get(j);
                ArrayList<Object> valuesList = record.getValuesList();
                if (valuesList.get(indexOfPrimary).equals(primaryKeyObject)) {
                    return record;
                }
            }
        }
        return null;
    }

    /**
     * Method prints all the records of a table
     * @param table table name
     */
    public void selectStarFromTable(Table table) {
        ArrayList<Record> recordList = getAllRecordsByTable(table);
        if (recordList != null) {
            if (recordList.size() == 0) {
                System.out.println("No record in this table.");
                System.out.println("SUCCESS");
            } else {
                for (String name : table.getAttriName_list()) {
                    System.out.print("  |  " + name + "  |  ");
                }
                System.out.print("\n");

                for (Record record : recordList) {
                    System.out.println(printRecord(record));
                }
            }
        } else {
            for (String name : table.getAttriName_list()) {
                System.out.print("  |  " + name + "  |  ");
            }
            System.out.print("\n");
        }
    }


    /**
     * Method inserts the record to table
     * @param inp input SQL
     * @param tablename table name
     * @return true if successfully otherwise false
     */
    public boolean insertRecordToTable(String inp, String tablename) {
        ArrayList<Pointer> pointerholder = new ArrayList<>();
        if (catalog.getTablesList() != null) {
            Table table = getTableByName(tablename);
            if (table != null) {
                int indexOfPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
                int startIndex = inp.indexOf("(");
                inp = inp.substring(startIndex, inp.length()-1);
                ArrayList<Record> recordArrayList = returnListofStringArrRecord(inp, table);
                //ArrayList<Record> recordArrayList = convertStringToRecordList(stringArrRecordList, table);
                if (recordArrayList != null) {
                    for (Record record : recordArrayList) {
                        boolean insertAlready = false;
                        if (table.getPageID_list().size() == 0) {
                            Page page = new Page(getPageIDList().size() + 1, table, this.db_loc,pointerholder);
                            page.getRecordList().add(record);
                            page.incCurrentPageSize(record, page.convertRecordToByteArr(record, table));
                            table.getPageID_list().add(getPageIDList().size() + 1);
                            table.increaseNumRecordBy1();

                            // needs the page size
                            if (page.getCurrent_page_size() > this.pageSize) {
                                System.err.println("This record is bigger than page size");
                                return false;
                            }

                            insertAlready = true;
                        } else {
                            ArrayList<Integer> originalPageIDList = new ArrayList<>(table.getPageID_list());

                            for (int m = 0; m < originalPageIDList.size(); m++) {

                                Page page = new Page(originalPageIDList.get(m), table, this.db_loc,pointerholder);

                                for (int i = 0; i < page.getRecordList().size(); i++) {
                                    Record tempRecord = page.getRecordList().get(i);
                                    Object recordPrimaryValue =  record.getValuesList().get(indexOfPrimaryKey);
                                    Object tempRecordPrimaryValue = tempRecord.getValuesList().get(indexOfPrimaryKey);
                                    int compare = compare2Records(recordPrimaryValue, tempRecordPrimaryValue);
                                    if (compare == 0) {
                                        System.err.println("Can't have duplicated primary key: " + printRecord(record));
                                        System.err.println("ERROR");
                                        return false;
                                    }

                                    // if primary key of new record does not duplicate
                                    if (compare < 0) {
                                        page.getRecordList().add(i, record);
                                        page.incCurrentPageSize(record, page.convertRecordToByteArr(record, table));
                                        table.increaseNumRecordBy1();
                                        int currentPagesize = page.getCurrent_page_size();
                                        //check if page overflows
                                        if (currentPagesize > page.getCurrent_page_size()) {
                                            int midpoint = page.getRecordList().size() / 2;
                                            ArrayList<Record> halfRecord = new ArrayList<>();
                                            // haldRecord stores the second half of the page when it overflows
                                            for (int j = midpoint; j < page.getRecordList().size(); j++) {
                                                halfRecord.add(page.getRecordList().get(j));
                                            }

                                            page.getRecordList().subList(midpoint, page.getRecordList().size()).clear();
                                            Page newPage = new Page(getPageIDList().size() + 1, table, this.db_loc,pointerholder);
                                            newPage.getRecordList().addAll(halfRecord);

                                            //decrement page 1 size after splitting
                                            //increment those into page 2
                                            for (Record rec : halfRecord) {
                                                page.decCurrentPageSize(rec, page.convertRecordToByteArr(rec, table));
                                                newPage.incCurrentPageSize(rec, newPage.convertRecordToByteArr(rec, table));
                                            }

                                            ArrayList<Table> tableListInCatalog = this.catalog.getTablesList();
                                            //add the new page into the table in catalog
                                            for (Table tbl : tableListInCatalog) {
                                                if (tbl.getTableName().equals(table.getTableName())) {
                                                    tbl.getPageID_list().add(m + 1, newPage.getPageID());
                                                }
                                            }
                                        }
                                        insertAlready = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if (!insertAlready) {
                            ArrayList<Integer> pageIDList = table.getPageID_list();
                            Page lastPage = new Page(pageIDList.get(pageIDList.size() - 1), table, this.db_loc,pointerholder);

                            lastPage.getRecordList().add(record);
                            lastPage.incCurrentPageSize(record, lastPage.convertRecordToByteArr(record, table));
                            table.increaseNumRecordBy1();
                            int currentPagesize = lastPage.getCurrent_page_size();
                            if (currentPagesize >this.pageSize ) {
                                int midpoint = lastPage.getRecordList().size() / 2;
                                ArrayList<Record> halfRecord = new ArrayList<>();
                                for (int j = midpoint; j < lastPage.getRecordList().size(); j++) {
                                    halfRecord.add(lastPage.getRecordList().get(j));
                                }
                                lastPage.getRecordList().subList(midpoint, lastPage.getRecordList().size()).clear();
                                Page newPage = new Page(getPageIDList().size() + 1, table, this.db_loc,pointerholder);
                                newPage.getRecordList().addAll(halfRecord);
                                for (Record rec : halfRecord) {
                                    lastPage.decCurrentPageSize(rec, lastPage.convertRecordToByteArr(rec, table));
                                    newPage.incCurrentPageSize(rec, newPage.convertRecordToByteArr(rec, table));
                                }

                                ArrayList<Table> tableListInCatalog = this.catalog.getTablesList();
                                for (Table tbl : tableListInCatalog) {
                                    if (tbl.getTableName().equals(table.getTableName())) {
                                        tbl.getPageID_list().add(newPage.getPageID());
                                        break;
                                        //tbl.getPageID_list().add(tableListInCatalog.size() + 1, newPage.getPageID());
                                    }
                                }
                            }
                        }
                    }

                    if (inp.split(",").length == recordArrayList.size()){
                        System.out.println("All record inserted.");
                        return true;
                    }else if(recordArrayList.size()>0){
                        System.out.println("The first " + recordArrayList.size() + " record has been inserted.");
                        return false;
                    }else{
                        return false;
                    }

                }
            }
            return false;
        }
        return false;
    }
}
