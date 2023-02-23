import java.util.ArrayList;
import java.util.List;

public class PageBuffer {
    private String db_loc;
    private int page_size;
    private ArrayList<Page> pagelistBuffer = new ArrayList<>();
    private int bufferSize;
    private StorageManager storageManager;

    //delete, get a page in memory
    public PageBuffer(String db_loc, int page_size, int bufferSize) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.bufferSize = bufferSize;
        this.storageManager = new StorageManager(db_loc, page_size, bufferSize);
    }

    public boolean quitProgram() {
        Catalog catalog =  this.storageManager.getCatalog();
        if (catalog != null) {
            byte[] catalogByteArr = catalog.convertCatalogToByteArr(catalog);
            String catalogPath = this.db_loc + "/catalog.txt";
            this.storageManager.writeByteArrToDisk(catalogPath, catalogByteArr);
        } else {
            System.err.println("Fails to write catalog to file!");
            System.err.println("ERROR");
            return false;
        }
        for (int i = 0; i < this.pagelistBuffer.size(); i++) {
            Page pageToWrite = this.pagelistBuffer.get(i);
            byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), pageToWrite.getTable(), this.page_size);
            String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
            this.storageManager.writeByteArrToDisk(path, byteArr);
        }

        return true;
    }

    //TODO
    public boolean insertRecordToTable(String inp, String tablename) {
        Table table = this.storageManager.getTableByName(tablename);
        int indexOfPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
        boolean insertAlready = false;
        ArrayList<String[]> stringArrRecordList = returnListofStringArrRecord(inp);
        ArrayList<Record> recordArrayList = convertStringToRecordList(stringArrRecordList, table);
        for (Record record : recordArrayList) {
            if (table.getPageID_list().size() == 0) {
                if (this.pagelistBuffer.size() > this.bufferSize) {
                    Page pageToWrite = this.pagelistBuffer.get(0);
                    byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), table, this.page_size);
                    String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
                    this.storageManager.writeByteArrToDisk(path, byteArr);
                    this.pagelistBuffer.remove(0);
                }
                Page page = new Page(getPageIDList().size()+1, table, this.db_loc);
                page.getRecordList().add(record);
                this.pagelistBuffer.add(page);
                insertAlready = true;
            } else {
                for (int m = 0; m < table.getPageID_list().size(); m++) {
                    if (this.pagelistBuffer.size() > this.bufferSize) {
                        Page pageToWrite = this.pagelistBuffer.get(0);
                        byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), table, this.page_size);
                        String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
                        this.storageManager.writeByteArrToDisk(path, byteArr);
                        this.pagelistBuffer.remove(0);
                    }
                    Page page = new Page(table.getPageID_list().get(m), table, this.db_loc);
                    this.pagelistBuffer.add(page);


                    for (int i = 0; i< page.getRecordList().size(); i++) {
                        Record tempRecord = page.getRecordList().get(i);
                        Object recordPrimaryValue = record.getValuesList().get(indexOfPrimaryKey);
                        Object tempRecordPrimaryValue = tempRecord.getValuesList().get(indexOfPrimaryKey);
                        int compare = compare2Records(recordPrimaryValue, tempRecordPrimaryValue);
                        if (compare == 0) {
                            System.err.println("Can't have duplicated primary key!");
                            System.err.println("ERROR");
                            return false;
                        }

                        if (compare < 0) {
                            page.getRecordList().add(i, record);
                            int currentPagesize = page.computeCurrentPagesize(page.getRecordList());
                            if (currentPagesize > this.page_size) {
                                int midpoint = page.getRecordList().size()/2;
                                ArrayList<Record> halfRecord = new ArrayList<>();
                                for (int j = midpoint; j < page.getRecordList().size(); j++) {
                                    halfRecord.add(page.getRecordList().get(j));
                                }
                                page.getRecordList().subList(midpoint, page.getRecordList().size()).clear();
                                if (this.pagelistBuffer.size() > this.bufferSize) {
                                    Page pageToWrite = this.pagelistBuffer.get(0);
                                    byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), table, this.page_size);
                                    String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
                                    this.storageManager.writeByteArrToDisk(path, byteArr);
                                    this.pagelistBuffer.remove(0);
                                }
                                Page newPage = new Page(getPageIDList().size()+1, table, this.db_loc);
                                newPage.getRecordList().addAll(halfRecord);
                                this.pagelistBuffer.add(newPage);
                                ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
                                for (Table tbl : tableListInCatalog) {
                                    if (tbl.getTableName().equals(table.getTableName())){
                                        tbl.getPageID_list().add(m+1, newPage.getPageID());
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
                Page lastPage = new Page(pageIDList.get(pageIDList.size()-1), table, this.db_loc);
                lastPage.getRecordList().add(record);
                int currentPagesize = lastPage.computeCurrentPagesize(lastPage.getRecordList());
                if (currentPagesize > this.page_size) {
                    int midpoint = lastPage.getRecordList().size()/2;
                    ArrayList<Record> halfRecord = new ArrayList<>();
                    for (int j = midpoint; j < lastPage.getRecordList().size(); j++) {
                        halfRecord.add(lastPage.getRecordList().get(j));
                    }
                    lastPage.getRecordList().subList(midpoint, lastPage.getRecordList().size()).clear();
                    if (this.pagelistBuffer.size() > this.bufferSize) {
                        Page pageToWrite = this.pagelistBuffer.get(0);
                        byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), table, this.page_size);
                        String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
                        this.storageManager.writeByteArrToDisk(path, byteArr);
                        this.pagelistBuffer.remove(0);
                    }
                    Page newPage = new Page(getPageIDList().size()+1, table, this.db_loc);
                    newPage.getRecordList().addAll(halfRecord);
                    this.pagelistBuffer.add(newPage);
                    ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
                    for (Table tbl : tableListInCatalog) {
                        if (tbl.getTableName().equals(table.getTableName())){
                            tbl.getPageID_list().add(tableListInCatalog.size()+1, newPage.getPageID());
                        }
                    }
                }
            }
        }
        return true;
    }

    public ArrayList<Integer> getPageIDList() {
        ArrayList<Integer> result = new ArrayList<>();
        ArrayList<Table> tableList = this.storageManager.getCatalog().getTablesList();
        for (Table table : tableList) {
            result.addAll(table.getPageID_list());
        }
        return result;
    }

    public ArrayList<Record> convertStringToRecordList(ArrayList<String[]> recordList, Table table) {
        ArrayList<Record> recordArrayList = new ArrayList<>();
        ArrayList<String> attriNameList = table.getAttriName_list();
        for (String[] stringarry : recordList) {
            if (stringarry.length != attriNameList.size()) {
                System.err.println("Number of attributes don't match. Expect " + attriNameList.size() + " attributes");
                System.err.println("ERROR");
                return null;
            } else {
                Record record = convertStringArrToRecord(stringarry, table);
                recordArrayList.add(record);
            }
        }
        return recordArrayList;
    }

    public Record convertStringArrToRecord(String[] strArr, Table table) {
        ArrayList<Object> valuesList = new ArrayList<>();
        String primaryKey = table.getPrimaryKeyName();
        ArrayList<String> attriTypeList = table.getAttriType_list();

        //check primaryKey if duplication and can't be null
        int indexOfPrimaryKey = getIndexOfColumn(primaryKey, table);
        for (int i = 0; i < strArr.length; i++) {
            char attrType = attriTypeList.get(i).charAt(0);
            if (attrType == '2') {
                if (i == indexOfPrimaryKey) {
                    String value = strArr[i];
                    if (value.equalsIgnoreCase("null")) {
                        System.err.println("Primary key can't be null");
                        System.err.println("ERROR");
                        return null;
                    }
                }
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
            } else if (attrType == '6') {
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
        return new Record(valuesList);
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

    // "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)"
    // FORMAT: insert into student values ("Alice" 1234 "86 Noel Drive Rochester NY14606" 3.2 true),("(A)" 1 "school" 2 "(false")
    // check: many tuples in 1 sql, check duplicate primary, check how many attributes, check the type of attribute,
    // check the length in varchar and char, check null, check if table name is exist
    public ArrayList<String[]> returnListofStringArrRecord(String inp) {
        ArrayList<String[]> recordList = new ArrayList<>();
        int startIndex = inp.indexOf("(");
        String inputTupesList = inp.substring(startIndex);
        if (inputTupesList.charAt(inputTupesList.length()-1) == ';') {
            inputTupesList = inputTupesList.substring(0, inputTupesList.length() - 1);
        }
        String[] splitString = splitString(inputTupesList);

        for (String str : splitString) {
            String tuple = removeFirstAndLastChar(str);
            String[] tupleArr = splitStringBySpaceButNotQuote(tuple);
            recordList.add(tupleArr);
        }
        return recordList;
    }

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

    private static boolean isBoolean(String str) {
        return str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false");
    }

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

    private static String removeFirstAndLastChar(String input) {
        if (input != null && input.length() > 2) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }

    private static String[] splitString(String input) {
        List<String> result = new ArrayList<>();
        boolean inQuote = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
                sb.append(c);
            } else if (c == ',' && !inQuote) {
                result.add(sb.toString().trim());
                sb = new StringBuilder();
            } else {
                sb.append(c);
            }
        }
        result.add(sb.toString().trim());
        return result.toArray(new String[0]);
    }

    private static String[] splitStringBySpaceButNotQuote(String input) {
        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\"') {
                inQuote = !inQuote;
                continue;
            } else if (c == ' ' && !inQuote) {
                result.add(sb.toString());
                sb = new StringBuilder();
                continue;
            }
            sb.append(c);
        }
        result.add(sb.toString());
        return result.toArray(new String[0]);
    }

}


