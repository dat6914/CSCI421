package Main;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This class is a page buffer that can get a page and remove a page that the least recently used (LRU)
 * when the buffer is full.
 * Description: The way the buffer works is like a Queue (FIFO): what page is accessed first will be added
 * to the queue and when the buffer is full, buffer will remove and write the first page in the queue
 * to the disk, so there will be room for another page loaded in memory.
 */
public class PageBuffer {
    private String db_loc;
    private int page_size;
    private ArrayList<Page> pagelistBuffer = new ArrayList<>();
    private int bufferSize;
    private StorageManager storageManager;

    private ArrayList<Table> tablesMarkedForDeletion = new ArrayList<>();


    public PageBuffer(String db_loc, int bufferSize, int page_size) {
        this.db_loc = db_loc;
        this.bufferSize = bufferSize;
        this.page_size = page_size;
        this.storageManager = new StorageManager(db_loc, this.page_size, bufferSize);
    }


    /**
     * Method get the page list in buffer
     *
     * @return arraylist of pages in buffer
     */
    public ArrayList<Page> getPagelistBuffer() {
        return this.pagelistBuffer;
    }


    /**
     * Method gets Main.Catalog
     *
     * @return catalog
     */
    public Catalog getCatalog() {
        return storageManager.getCatalog();
    }

    /**
     * Method saves the pages buffer to disk and catalog file to disk
     *
     * @param storageManager storage manager
     * @param pagelistBuffer arraylist of page in buffer
     * @return true if successful otherwise false
     */
    public boolean quitProgram(StorageManager storageManager, ArrayList<Page> pagelistBuffer) throws IOException {
        Catalog catalog = storageManager.getCatalog();
        if (catalog != null) {
            byte[] catalogByteArr = catalog.convertCatalogToByteArr(catalog);
            String catalogPath = this.db_loc + "/catalog.txt";
            this.storageManager.writeByteArrToDisk(catalogPath, catalogByteArr);

            // dropping tables
            for (int i = 0; i < tablesMarkedForDeletion.size(); i++) {
                Table table = tablesMarkedForDeletion.get(i);
                String tableName = table.getTableName();

                // check if any pages in the buffer are from the table that is being deleted
                for (int j = 0; j < pagelistBuffer.size(); j++) {
                    Page page = pagelistBuffer.get(j);

                    if (page.getTablename().equals(tableName)) {
                        pagelistBuffer.remove(page);
                    }
                }

                //this.storageManager.getCatalog().getTablesList().remove(table);
                Files.deleteIfExists(Paths.get(this.db_loc + "/" + tableName + ".txt"));

                //String path = this.db_loc + "/Tables/" + tableName + ".txt";
                //File file = new File(path);
                //file.delete();
            }

            for (int i = 0; i < pagelistBuffer.size(); i++) {
                Page pageToWrite = pagelistBuffer.get(i);
                String tableName = pageToWrite.getTablename();
                Table table = catalog.getTableByName(tableName);
                int numPageInTable = table.getPageID_list().size();

                byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite, this.page_size);

                String path = this.db_loc + "/Tables/" + tableName + ".txt";
                PageBuffer.writeToDiskWithRandomAccess(path, pageToWrite, 0, this.page_size, numPageInTable);
                this.storageManager.writeByteArrToDisk(path, byteArr);
            }


        } else {
            System.err.println("Failed to write catalog to file!");
            System.err.println("ERROR");
            return false;
        }
        return true;
    }


    /**
     * Write certain number of bytes to disk given a specific offset.
     *
     * @param path     file path
     * @param page     page to be written
     * @param offset   offset to write in disk
     * @param pageSize page size
     */
    public static void writeToDiskWithRandomAccess(String path, Page page, int offset, int pageSize, int numPageInTable) {
        File file = new File(path);

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
            byte[] numPageInTableByte = ByteBuffer.allocate(Integer.BYTES).putInt(numPageInTable).array();
            byte[] data = page.convertPageToByteArr(page, pageSize);
            randomAccessFile.seek(0);
            // write in the number of pages in the first 4 bytes
            randomAccessFile.write(numPageInTableByte);
            randomAccessFile.seek(offset);
            // write in whatever page bytearray to its designated index
            randomAccessFile.write(data);
            randomAccessFile.close();
        } catch (IOException i) {
            System.err.println("An error occurred while writing to the file.");
            i.printStackTrace();
            System.err.println("ERROR");
        }

    }

    /**
     * Read certain number of bytes at a given offset from the disk. And deserialize it into a page
     *
     * @param path     path to file
     * @param offset   offset of where page exists in hardware
     * @param pageSize page size that was allocated to it.
     * @return a page
     */
    public static Page readFromDiskWithRandomAccess(String path, int offset, int pageSize) {
        File file = new File(path);
        if (file.exists()) {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
                randomAccessFile.seek(offset);
                byte[] pageByteArray = new byte[pageSize];
                randomAccessFile.readFully(pageByteArray);
                Page page = Page.convertByteArrToPage(pageByteArray);
                return page;
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file.");
                e.printStackTrace();
                System.err.println("ERROR");
            }
        } else {
            System.err.println("Table not exist");
            return null;
        }
        return null;
    }


    /**
     * Method inserts the record to table
     *
     * @param inp       input SQL
     * @param tablename table name
     * @return true if successfully otherwise false
     */
    public boolean insertRecordToTable(String inp, String tablename) {
        if (this.storageManager.getCatalog().getTablesList() != null) {
            Table table = this.storageManager.getTableByName(tablename);
            if (table != null) {
                int indexOfPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
                int startIndex = inp.indexOf("(");
                inp = inp.substring(startIndex, inp.length() - 1);
                ArrayList<Record> recordArrayList = returnListofStringArrRecord(inp, table);
                //ArrayList<Main.Record> recordArrayList = convertStringToRecordList(stringArrRecordList, table);
                if (recordArrayList != null) {
                    for (Record record : recordArrayList) {
                        boolean insertAlready = false;
                        if (table.getPageID_list().size() == 0) {
                            removeLRUFromBufferIfOverflow(getCatalog());
                            Page page = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
                            page.getRecordList().add(record);
                            page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
                            table.getPageID_list().add(getPageIDList().size() + 1);
                            table.increaseNumRecordBy1();

                            // change split
                            if (record.convertRecordToByteArr(record).length > this.page_size) {
                                System.out.println("page size" + this.page_size);
                                System.err.println("This record is bigger than page size");
                                return false;
                            }

                            //check if the page is in the buffer
                            // if not then add to pagelist buffer
                            if (!this.pagelistBuffer.contains(page)) {
                                this.pagelistBuffer.add(page);
                                removeLRUFromBufferIfOverflow(getCatalog());
                                //else then remove that page and add back to the last (this is the most recently use)
                            } else {
                                int indexPage = this.pagelistBuffer.indexOf(page);
                                if (indexPage != -1) {
                                    this.pagelistBuffer.remove(indexPage);
                                    this.pagelistBuffer.add(page);
                                    removeLRUFromBufferIfOverflow(getCatalog());
                                }

                            }
                            insertAlready = true;
                        } else {
                            ArrayList<Integer> originalPageIDList = new ArrayList<>(table.getPageID_list());

                            for (int m = 0; m < originalPageIDList.size(); m++) {

                                Page page = new Page(originalPageIDList.get(m), table.getTableName(), this.db_loc);
                                if (!this.pagelistBuffer.contains(page)) {
                                    this.pagelistBuffer.add(page);
                                    removeLRUFromBufferIfOverflow(getCatalog());
                                } else {
                                    for (Page tempPage : this.pagelistBuffer) {
                                        if (tempPage.equals(page)) {
                                            page = tempPage;
                                            break;
                                        }
                                    }
                                    int indexPage = this.pagelistBuffer.indexOf(page);
                                    if (indexPage != -1) {
                                        this.pagelistBuffer.remove(indexPage);
                                        this.pagelistBuffer.add(page);
                                        removeLRUFromBufferIfOverflow(getCatalog());
                                    }
                                }

                                for (int i = 0; i < page.getRecordList().size(); i++) {
                                    Record tempRecord = page.getRecordList().get(i);
                                    Object recordPrimaryValue = record.getValuesList().get(indexOfPrimaryKey);
                                    Object tempRecordPrimaryValue = tempRecord.getValuesList().get(indexOfPrimaryKey);
                                    int compare = compare2Records(recordPrimaryValue, tempRecordPrimaryValue);
                                    if (compare == 0) {
                                        System.err.println("Can't have duplicated primary key: " + printRecord(record));
                                        System.err.println("ERROR");
                                        return false;
                                    }

                                    // if primary key of new record does not duplicate
                                    // split page
                                    if (compare < 0) {
                                        System.out.println(this.page_size);
                                        page.getRecordList().add(i, record);
                                        page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
                                        table.increaseNumRecordBy1();
                                        int currentPagesize = page.getCurrent_page_size();
                                        //check if page overflows
                                        if (currentPagesize > this.page_size) {
                                            int midpoint = page.getRecordList().size() / 2;
                                            ArrayList<Record> halfRecord = new ArrayList<>();
                                            // haldRecord stores the second half of the page when it overflows
                                            for (int j = midpoint; j < page.getRecordList().size(); j++) {
                                                halfRecord.add(page.getRecordList().get(j));
                                            }

                                            page.getRecordList().subList(midpoint, page.getRecordList().size()).clear();
                                            Page newPage = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
                                            newPage.getRecordList().addAll(halfRecord);

                                            //decrement page 1 size after splitting
                                            //increment those into page 2
                                            for (Record rec : halfRecord) {
                                                page.decCurrentPageSize(rec, record.convertRecordToByteArr(rec));
                                                newPage.incCurrentPageSize(rec, record.convertRecordToByteArr(rec));
                                            }

                                            //add new page into buffer
                                            this.pagelistBuffer.add(newPage);
                                            //check if buffer full
                                            removeLRUFromBufferIfOverflow(getCatalog());
                                            ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
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
                            Page lastPage = new Page(pageIDList.get(pageIDList.size() - 1), table.getTableName(), this.db_loc);
                            for (Page tempPage : this.pagelistBuffer) {
                                if (tempPage.equals(lastPage)) {
                                    lastPage = tempPage;
                                    break;
                                }
                            }
                            lastPage.getRecordList().add(record);
                            lastPage.incCurrentPageSize(record, record.convertRecordToByteArr(record));
                            table.increaseNumRecordBy1();
                            int currentPagesize = lastPage.getCurrent_page_size();
                            if (currentPagesize > this.page_size) {
                                int midpoint = lastPage.getRecordList().size() / 2;
                                ArrayList<Record> halfRecord = new ArrayList<>();
                                for (int j = midpoint; j < lastPage.getRecordList().size(); j++) {
                                    halfRecord.add(lastPage.getRecordList().get(j));
                                }
                                lastPage.getRecordList().subList(midpoint, lastPage.getRecordList().size()).clear();
                                Page newPage = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
                                newPage.getRecordList().addAll(halfRecord);
                                for (Record rec : halfRecord) {
                                    lastPage.decCurrentPageSize(rec, record.convertRecordToByteArr(rec));
                                    newPage.incCurrentPageSize(rec, record.convertRecordToByteArr(rec));
                                }

                                this.pagelistBuffer.add(newPage);
                                removeLRUFromBufferIfOverflow(getCatalog());
                                ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
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

                    if (inp.split(",").length == recordArrayList.size()) {
                        System.out.println("All record inserted.");
                        return true;
                    } else if (recordArrayList.size() > 0) {
                        System.out.println("The first " + recordArrayList.size() + " record has been inserted.");
                        return false;
                    } else {
                        return false;
                    }

                }
            }
            return false;
        }
        return false;
    }


    /**
     * Method checks if buffer is full. If it is full then write the first page in the arraylist
     * of pages to the disk
     *
     * @param catalog catalog
     */
    private void removeLRUFromBufferIfOverflow(Catalog catalog) {
        if (this.pagelistBuffer.size() == this.bufferSize) {
            Page pageToWrite = this.pagelistBuffer.get(0);
            String tableName = pageToWrite.getTablename();
            Table table = catalog.getTableByName(tableName);
            String path = this.db_loc + "/Tables/" + tableName + ".txt";
            int numPageInTable = table.getPageID_list().size();
            int idxOfPage = table.getPageID_list().indexOf(pageToWrite.getPageID());
            int offset = Integer.BYTES + (page_size * idxOfPage);
            PageBuffer.writeToDiskWithRandomAccess(path, pageToWrite, offset, this.page_size, numPageInTable);
            this.pagelistBuffer.remove(0);
        }
    }


    /**
     * Method gets the list of pageID
     *
     * @return arraylist of pageID
     */
    public ArrayList<Integer> getPageIDList() {
        ArrayList<Integer> result = new ArrayList<>();
        ArrayList<Table> tableList = this.storageManager.getCatalog().getTablesList();
        for (Table table : tableList) {
            result.addAll(table.getPageID_list());
        }
        return result;
    }


    /**
     * Method converts arraylist of record string to arraylist of records
     *
     * @param recordList arraylist of record string
     * @param table      table that records belong to
     * @return arraylist of records
     */
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
                if (record == null) {
                    return recordArrayList;
                }
                recordArrayList.add(record);
            }
        }
        return recordArrayList;
    }


    /**
     * Method converts byte array of record to record object
     *
     * @param strArr byte array of record
     * @param table  table
     * @return record object
     */
    public Record convertStringArrToRecord(String[] strArr, Table table) {
        ArrayList<Object> valuesList = new ArrayList<>();
        String primaryKey = table.getPrimaryKeyName();
        ArrayList<String> attriTypeList = table.getAttriType_list();

        //check primaryKey if duplication and can't be null
        int indexOfPrimaryKey = getIndexOfColumn(primaryKey, table);
        for (int i = 0; i < strArr.length; i++) {
            char attrType = attriTypeList.get(i).charAt(0);
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
                if (!strArr[i].substring(0, 1).equals("\"") || !strArr[i].substring(strArr[i].length() - 1, strArr[i].length()).equals("\"")) {
                    System.err.println("Expect quotations for string values");
                    System.err.println("ERROR");
                    return null;
                }
                strArr[i] = strArr[i].substring(1, strArr[i].length() - 1);

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
                if (!strArr[i].substring(0, 1).equals("\"") || !strArr[i].substring(strArr[i].length() - 1, strArr[i].length()).equals("\"")) {
                    System.err.println("Expect quotations for string values");
                    System.err.println("ERROR");
                    return null;
                }
                // take out the quote in the front and at the end.
                strArr[i] = strArr[i].substring(1, strArr[i].length() - 1);

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
        return new Record(valuesList, new ArrayList<String>());
    }


    /**
     * Method gets the index of attribute name in the attribute name list
     *
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

    /**
     * Method return the input record string to the list of string array
     *
     * @param inputTupesList record string input
     * @return arraylist of array string (one string[] is the record string)
     */
    public ArrayList<Record> returnListofStringArrRecord(String inputTupesList, Table table) {
        ArrayList<Record> recordList = new ArrayList<>();
        String[] splitString = inputTupesList.split(",");

        for (String str : splitString) {
            if (!str.substring(0, 1).equals("(") || !str.substring(str.length() - 1, str.length()).equals(")")) {
                return null;
            }
            String tupleAfterRemoveParenthesis = removeFirstAndLastChar(str);
            String[] tupleSplitBySpace = tupleAfterRemoveParenthesis.split(" ");
            if (tupleSplitBySpace.length != table.getAttriName_list().size()) {
                // number of attributes don't match
                return null;
            }

            Record r = convertStringArrToRecord(tupleSplitBySpace, table);
            if (r != null) {
                recordList.add(r);
            }
        }
        return recordList;
    }


    /**
     * Method compares 2 Objects
     *
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
     * Method checks if a string is boolean
     *
     * @param str string
     * @return true if a boolean otherwise false
     */
    private static boolean isBoolean(String str) {
        return str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false");
    }


    /**
     * Method checks if a string is double
     *
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
     *
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
     * Method removes the first and last of a string
     *
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
     * Method gets the storage manager
     *
     * @return storage manager
     */
    public StorageManager getStorageManager() {
        return this.storageManager;
    }

    /**
     * check if pageid exist in pagebuffer.
     * @param pageid pageid
     * @param pageBuffer pagebuffer
     * @return page
     */
    public Page findPageInBuffer(int pageid, PageBuffer pageBuffer) {
        for (Page page : pageBuffer.getPagelistBuffer()) {
            if (page.getPageID() == pageid) {
                return page;
            }
        }
        return null;
    }

    /**
     * Method prints all the records of a table
     *
     * @param table table name
     */
    public void selectStarFromTable(Table table) {
        // pull from pagebuffer if it doesn't exist then read from hardware
        // check if record list.size is not 0
        // else get the table attribute name list and print
        ArrayList<Integer> pageid_list = table.getPageID_list();
        if (pageid_list.size() == 0) {
            for (String attributeName : table.getAttriName_list()) {
                System.out.println("  |  " + attributeName + "  |  ");
            }
            System.out.println("\n");
        }
        int index = 4;
        for (int i = 0; i < pageid_list.size(); i++) {
            Page page = findPageInBuffer(table.getPageID_list().get(i), this);
            if (page == null) {
                page = readFromDiskWithRandomAccess(table.getTableName(), (index * i) + 4, this.page_size);
                if (page.getRecordList() != null) {
                    if (page.getRecordList().size() == 0) {
                        System.out.println("No record in table: " + table.getTableName());
                        System.out.println("SUCCESS");
                    } else {
                        for (String attributeName : table.getAttriName_list()) {
                            System.out.println("  |  " + attributeName + "  |  ");
                        }
                        System.out.println("\n");
                        for (Record record : page.getRecordList()) {
                            System.out.println(printRecord(record));
                        }
                    }
                } else {
                    for (String attributeName : table.getAttriName_list()) {
                        System.out.println("  |  " + attributeName + "  |  ");
                    }
                    System.out.println("\n");
                }

            } else{
                if (page.getRecordList() != null) {
                    if (page.getRecordList().size() == 0) {
                        System.out.println("No record in table: " + table.getTableName());
                        System.out.println("SUCCESS");
                    } else {
                        for (String attributeName : table.getAttriName_list()) {
                            System.out.println("  |  " + attributeName + "  |  ");
                        }
                        System.out.println("\n");
                        for (Record record : page.getRecordList()) {
                            System.out.println(printRecord(record));
                        }
                    }
                } else {
                    for (String attributeName : table.getAttriName_list()) {
                        System.out.println("  |  " + attributeName + "  |  ");
                    }
                    System.out.println("\n");
                }
            }
        }
    }



    /**
     * Method prints all the records of a table
     * @param table table name
     */
//    public void selectStarFromTable(Table table) {
//        ArrayList<Record> recordList = getAllRecordsByTable(table);
//        if (recordList != null) {
//            if (recordList.size() == 0) {
//                System.out.println("No record in this table.");
//                System.out.println("SUCCESS");
//            } else {
//                for (String name : table.getAttriName_list()) {
//                    System.out.print("  |  " + name + "  |  ");
//                }
//                System.out.print("\n");
//
//                for (Record record : recordList) {
//                    System.out.println(printRecord(record));
//                }
//            }
//        } else {
//            for (String name : table.getAttriName_list()) {
//                System.out.print("  |  " + name + "  |  ");
//            }
//            System.out.print("\n");
//        }
//    }


    /**
     * Method get all records in the table by table name
     * @param table table
     * @return arraylist of records of the table
     */
    public ArrayList<Record> getAllRecordsByTable(Table table) {
        ArrayList<Record> result = new ArrayList<>();
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table.getTableName(), this.db_loc);
            if (this.pagelistBuffer.contains(page)) {
                for (Page tempPage : this.pagelistBuffer) {
                    if (tempPage.equals(page)) {
                        page = tempPage;
                        break;
                    }
                }
            } else {
                this.pagelistBuffer.add(page);
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
     * Method prints out the Main.Record
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
     * Method gets the page by pageID of the table by table name
     * @param table table
     * @param pageNum pageNum
     */
    public boolean getPageByTableAndPageNumber(Table table, int pageNum) {
        ArrayList<String> attrName = table.getAttriName_list();
        ArrayList<Integer> pageList = table.getPageID_list();
        for (int i = 0; i < pageList.size(); i++) {
            if (i == pageNum - 1) {
                Page page = new Page(pageList.get(i), table.getTableName(), this.db_loc);
                if (this.pagelistBuffer.contains(page)) {
                    for (Page tempPage : this.pagelistBuffer) {
                        if (tempPage.equals(page)) {
                            page = tempPage;
                            break;
                        }
                    }
                } else {
                    this.pagelistBuffer.add(page);
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
     * Method gets record by primary key and table name
     * @param primaryKeyValue primary key of record
     * @param table table
     * @return record
     */
    public Record getRecordByPrimaryKey(String primaryKeyValue, Table table) {
        Object primaryKeyObject = this.storageManager.convertPrimaryValueToItsType(primaryKeyValue, table);
        String primaryKeyName = table.getPrimaryKeyName();
        int indexOfPrimary = getIndexOfColumn(primaryKeyName, table);
        ArrayList<Integer> pageIDlist = table.getPageID_list();
        for (int i = 0; i < pageIDlist.size(); i++) {
            int pageID = pageIDlist.get(i);
            Page page = new Page(pageID, table.getTableName(), this.db_loc);

            if (this.pagelistBuffer.contains(page)) {
                for (Page tempPage : this.pagelistBuffer) {
                    if (tempPage.equals(page)) {
                        page = tempPage;
                        break;
                    }
                }
            }

            else {
                this.pagelistBuffer.add(page);
                removeLRUFromBufferIfOverflow(getCatalog());
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
     * Method return string of record by primaryKey
     * @param record record
     * @return string of record
     */
    public String getRecordByPrimary(Record record) {
        return printRecord(record);
    }

    public boolean dropTable(String tableName) {
        Table table = this.storageManager.getTableByName(tableName);

        if (table != null) {
            tablesMarkedForDeletion.add(table);
            this.storageManager.getCatalog().getTablesList().remove(table);

            return true;
        }
        return false;
    }

    public boolean dropAttribute(String attrName, String tableName) {
        Table table = this.storageManager.getTableByName(tableName);

        //drop attribute from table by just making a new table with the same name but without the attribute
        if (table != null) {
            //create copy of table


        }

        return false;
    }
}


