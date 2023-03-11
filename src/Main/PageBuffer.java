package Main;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
    private ArrayList<Integer> pageIDListOfCatalog = new ArrayList<>(); //keep track of pageID, for unique

    public PageBuffer(String db_loc, int bufferSize, int page_size) {
        this.db_loc = db_loc;
        this.bufferSize = bufferSize;
        this.page_size = page_size;
        this.storageManager = new StorageManager(db_loc, this.page_size, bufferSize);
        this.pageIDListOfCatalog = getPageIDOfWholeCatalog();
    }

    public ArrayList<Integer> getPageIDListOfCatalog(){
        return this.pageIDListOfCatalog;
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
     * Method displays the display schema
     * @param location location of database
     * @param pageSize size of page (byte)
     * @param bufferSize    size of buffer (number of pages)
     * @param catalog catalog needs to be displayed
     */
    public void displaySchema(String location, int pageSize, int bufferSize, Catalog catalog) {
        System.out.println("DB location: " + location);
        System.out.println("Page Size: " + pageSize);
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
     * Method saves the pages buffer to disk and catalog file to disk
     *
     * @param storageManager storage manager
     * @param pagelistBuffer arraylist of page in buffer
     * @return true if successfully otherwise false
     */
    public boolean quitProgram(StorageManager storageManager, ArrayList<Page> pagelistBuffer) {
        Catalog catalog = storageManager.getCatalog();
        if (catalog != null) {
            byte[] catalogByteArr = catalog.convertCatalogToByteArr(catalog);
            String catalogPath = this.db_loc + "/catalog.txt";
            this.storageManager.writeByteArrToDisk(catalogPath, catalogByteArr);
            for (int i = 0; i < pagelistBuffer.size(); i++) {
                Page pageToWrite = pagelistBuffer.get(i);
                String tableName = pageToWrite.getTablename();
                Table table = catalog.getTableByName(tableName);
                int numPageInTable = table.getPageID_list().size();

                byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite, this.page_size);

                String path = this.db_loc + "/Tables/" + tableName + ".txt";
                ArrayList<Integer> pageIDListFromDisk = getPageIDListFromDiskWithRA(table);
                File file = new File(path);
                if (file.exists()) {
                    //check if this is a new page or not
                    if (pageIDListFromDisk.contains(pageToWrite.getPageID())) {
                        int indexPage = pageIDListFromDisk.indexOf(pageToWrite.getPageID());
                        int offset = indexPage * page_size;
                        writeToDiskWithRandomAccess(path, pageToWrite,offset , this.page_size, numPageInTable);
                    } else {
                        int offset = table.getPageID_list().size() * page_size;
                        writeToDiskWithRandomAccess(path, pageToWrite, offset, this.page_size, numPageInTable);
                    }
                } else {
                    try {
                        file.createNewFile();
                        writeToDiskWithRandomAccess(path, pageToWrite, 0, this.page_size, numPageInTable);
                    } catch (IOException e) {
                        System.err.println("Fails to write page to new table file.");
                        e.printStackTrace();
                        System.err.println("ERROR");
                        return false;
                    }

                }

                //this.storageManager.writeByteArrToDisk(path, byteArr);
            }

        } else {
            System.err.println("Fails to write catalog to file!");
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
    public void writeToDiskWithRandomAccess(String path, Page page, int offset, int pageSize, int numPageInTable) {
        File file = new File(path);

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
            byte[] data = page.convertPageToByteArr(page, pageSize);
            randomAccessFile.seek(offset);
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
     * @param tableName     path to file
     * @param offset   offset of where page exists in hardware
     * @param pageSize page size that was allocated to it.
     * @return a page
     */
    public Page readFromDiskWithRandomAccess(int pageID, String tableName, int offset, int pageSize) {
        String path = this.db_loc + "/Tables/" + tableName + ".txt";
        File file = new File(path);
        if (file.exists()) {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
                randomAccessFile.seek(offset);
                byte[] pageByteArray = new byte[pageSize];
                randomAccessFile.readFully(pageByteArray);
                Page tempPage = new Page(pageID, tableName, this.db_loc);
                Page page = tempPage.convertByteArrToPage(pageByteArray, pageID, tableName);
                return page;
            } catch (IOException e) {
                System.err.println("An error occurred while reading from the file.");
                e.printStackTrace();
                System.err.println("ERROR");
            }
        } else {
            System.err.println("Table file does not exist!");
            return null;
        }
        return null;
    }

    public ArrayList<Integer> getPageIDListFromDiskWithRA(Table table) {
        ArrayList<Integer> pageIDList = new ArrayList<>();
        String path = this.db_loc + "/Tables/" + table.getTableName() + ".txt";
        File file = new File(path);
        if (file.exists()) {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
                int offset = 0;
                for (int i = 0; i < table.getPageID_list().size(); i++) {
                    randomAccessFile.seek(offset);
                    byte[] pageIDArr = new byte[Integer.BYTES];
                    randomAccessFile.readFully(pageIDArr);
                    int pageID = ByteBuffer.wrap(pageIDArr).getInt();
                    pageIDList.add(pageID);
                    offset = offset + this.page_size;
                }
            } catch (IOException e) {
                System.err.println("An error occurred while reading from the file.");
                e.printStackTrace();
                System.err.println("ERROR");
                return null;
            }
        }
        return pageIDList;
    }

    public ArrayList<Record> returnListofStringArrRecord(String insertSQL, Table table) {
        ArrayList<String[]> insertSQLAfterSplit = splitInsertCommandInput(insertSQL, table);
        ArrayList<Record> recordInsertList = new ArrayList<>();
        for (String[] value : insertSQLAfterSplit) {
            Record record = validateDataType(value, table.getAttriType_list());
            if (record == null) {
                recordInsertList.add(null);
            }
            else {
                recordInsertList.add(record);
            }
        }
        return recordInsertList;
    }


    public boolean insertRecordsToTable(String insertSQL, Table table) {
        ArrayList<Record> recordList = returnListofStringArrRecord(insertSQL, table);
        if(recordList!= null) {
            int count = 0;
            for (Record record : recordList) {
                if (record != null) {
                    if (insertARecordToTable(record, table)) {
                        count++;
                    } else {
                        break;
                    }
                }
            }
            if (count == recordList.size()){
                return true;
            }
        }
        return false;
    }


    public boolean insertARecordToTable(Record record, Table table) {
        ArrayList<Integer> pageIDList = table.getPageID_list();
        if (pageIDList.size() == 0) {
            TableFile tableFile = new TableFile(table, this.page_size, this.db_loc);
            ArrayList<Record> recordArrayList = new ArrayList<>();
            recordArrayList.add(record);
            int pageID = findMaxinumNumInList(pageIDListOfCatalog) + 1;
            String tableName = table.getTableName();
            Page page = new Page(pageID, tableName, recordArrayList);
            page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
            table.increaseNumRecordBy1();
            tableFile.getPageList().add(page);
            getCatalog().getTableByName(tableName).getPageID_list().add(pageID);
            removeLRUFromBufferIfOverflow(this.getCatalog());
            this.pagelistBuffer.add(page);
            return true;
        } else {
            ArrayList<Integer> pageIDListFromTableFile = getPageIDListFromDiskWithRA(table);
            ArrayList<Integer> pageIDListFromCatalogTable = table.getPageID_list();
            for (int i = 0; i < pageIDListFromCatalogTable.size(); i++) {
                int pageID = pageIDListFromCatalogTable.get(i);
                Page page = new Page(pageID);
                boolean flag = false;
                for (Page tempPage : this.pagelistBuffer) {
                    if (tempPage.getPageID() == page.getPageID()) {
                        page = tempPage;
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    int indexPage = pageIDListFromTableFile.indexOf(pageID);
                    int offset = indexPage * this.page_size;
                    page = readFromDiskWithRandomAccess(pageID, table.getTableName(), offset, this.page_size);
                }
                boolean containPage = false;
                for(Page tempPage : this.pagelistBuffer) {
                    if (tempPage.getPageID() == pageID) {
                        containPage = true;
                        break;
                    }
                }
                if (!containPage) {
                    this.pagelistBuffer.add(page);
                }
                removeLRUFromBufferIfOverflow(this.getCatalog());
                for (int j = 0; j < page.getRecordList().size(); j++) {
                    Record tempRecord = page.getRecordList().get(j);
                    int indexOfPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
                    Object recordPrimaryValue = record.getValuesList().get(indexOfPrimaryKey);
                    Object tempRecordPrimaryValue = tempRecord.getValuesList().get(indexOfPrimaryKey);
                    int compare = compare2Records(recordPrimaryValue, tempRecordPrimaryValue);
                    if (compare == 0) {
                        System.err.println("Can't have duplicated primary key: " + printRecord(record));
                        System.err.println("ERROR");
                        return false;
                    } else if (compare < 0) {
                        page.getRecordList().add(i, record);
                        page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
                        table.increaseNumRecordBy1();
                        int currentPagesize = page.getCurrent_page_size();
                        //check if page overflows
                        if (currentPagesize > this.page_size) {
                            int midpoint = page.getRecordList().size() / 2;
                            ArrayList<Record> halfRecord = new ArrayList<>();
                            // haldRecord stores the second half of the page when it overflows
                            for (int k = midpoint; k < page.getRecordList().size(); k++) {
                                halfRecord.add(page.getRecordList().get(k));
                            }

                            page.getRecordList().subList(midpoint, page.getRecordList().size()).clear();
                            int newPageID = findMaxinumNumInList(pageIDListOfCatalog)+1;
                            Page newPage = new Page(newPageID, table.getTableName(), this.db_loc);
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
                                    tbl.getPageID_list().add(i + 1, newPage.getPageID());
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            //
            int pageIDLastPage = pageIDListFromCatalogTable.get(pageIDListFromCatalogTable.size()-1);
            Page lastPage = new Page(pageIDLastPage);
            boolean flag = false;
            for (Page tempPage : pagelistBuffer) {
                if (tempPage.getPageID() == lastPage.getPageID()) {
                    lastPage = tempPage;
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                int indexLastPage = pageIDListFromTableFile.indexOf(pageIDLastPage);
                int offsetLastPage = indexLastPage * this.page_size;
                lastPage = readFromDiskWithRandomAccess(pageIDLastPage, table.getTableName(), offsetLastPage, this.page_size);
            }

            lastPage.getRecordList().add(record);
            lastPage.incCurrentPageSize(record, record.convertRecordToByteArr(record));
            table.increaseNumRecordBy1();

            boolean containPage = false;
            for(Page tempPage : this.pagelistBuffer) {
                if (tempPage.getPageID() == pageIDLastPage) {
                    containPage = true;
                    break;
                }
            }
            if (!containPage) {
                this.pagelistBuffer.add(lastPage);
            }
            //check if buffer full
            removeLRUFromBufferIfOverflow(getCatalog());
            int currentPagesize = lastPage.getCurrent_page_size();
            if (currentPagesize > this.page_size) {
                int midpoint = lastPage.getRecordList().size() / 2;
                ArrayList<Record> halfRecord = new ArrayList<>();
                for (int j = midpoint; j < lastPage.getRecordList().size(); j++) {
                    halfRecord.add(lastPage.getRecordList().get(j));
                }
                lastPage.getRecordList().subList(midpoint, lastPage.getRecordList().size()).clear();
                int newPageID = findMaxinumNumInList(pageIDListOfCatalog)+1;
                Page newPage = new Page(newPageID, table.getTableName(), this.db_loc);
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
                        return true;
                    }
                }
            } else {
                return true;
            }
        }
        return false;
    }


    public int findMaxinumNumInList(ArrayList<Integer> numList){
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < numList.size(); i++) {
            if (numList.get(i) > max) {
                max = numList.get(i);
            }
        }
        return max;
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
            if (table.getPageID_list().contains(pageToWrite.getPageID())) {
                int idxOfPage = table.getPageID_list().indexOf(pageToWrite.getPageID());
                int offset = page_size * idxOfPage;
                writeToDiskWithRandomAccess(path, pageToWrite, offset, this.page_size, numPageInTable);
                this.pagelistBuffer.remove(0);
            } else {
                int offset = page_size * table.getPageID_list().size();
                writeToDiskWithRandomAccess(path, pageToWrite, offset, this.page_size, numPageInTable);
                this.pagelistBuffer.remove(0);
            }
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


    public ArrayList<String[]> splitInsertCommandInput(String insertSQL, Table table) {
        ArrayList<String[]> result = new ArrayList<>();

        //get String after the word "values"
        int indexOfValues = insertSQL.indexOf("values");
        insertSQL = insertSQL.substring(indexOfValues+7, insertSQL.length());

        //remove the ; at the end
        insertSQL = insertSQL.substring(0, insertSQL.length()-1);

        //split the string by ',' but not ',' in the quote
        String[] valuesArr = insertSQL.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        //remove the first '(' and last ')' in the every value
        //and check if there is a space in front of quote, if not then add a space in front of quote
        for (int i = 0; i < valuesArr.length; i++) {
            String temp1 = valuesArr[i].trim();
            String temp = removeFirstAndLastChar(temp1);
            temp = temp.replaceAll("(?<!\\s)\"|\"(?!\\s)", " \"");
            valuesArr[i] = temp;
        }


        ArrayList<String> attributeTypeList = table.getAttriType_list();
        for (int i = 0; i < valuesArr.length; i++) {
            //for every value string: split by space but not space in the quote
            String[] splitStr = valuesArr[i].split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            for (int m = 0; m < splitStr.length; m++) {
                String trimmedString = splitStr[m].trim();
                if (trimmedString.isEmpty()) {
                    System.arraycopy(splitStr, m + 1, splitStr, m, splitStr.length - m - 1);
                    m--; // to re-check the same index after shifting the elements left
                    splitStr = Arrays.copyOf(splitStr, splitStr.length - 1); // to resize the array
                }
            }
            result.add(splitStr);
        }

        return result;
    }


    public Record validateDataType(String[] valueArr, ArrayList<String> attributeList) {
        ArrayList<Object> result = new ArrayList<>();
        if (valueArr.length != attributeList.size()) {
            System.err.println("Number of attributes don't match. Expect " + attributeList.size() + " attributes");
            System.err.println("ERROR");
            return null;
        } else {
            for(int i = 0; i < attributeList.size(); i++) {
                String type = attributeList.get(i).substring(0,1);
                String value = valueArr[i];
                switch (type) {
                    case "2":
                        if (value.contains("\"")) {
                            System.err.println("Expect " + value + " is a boolean but got a string");
                            System.err.println("ERROR");
                            return null;
                        } else {
                            if (isBoolean(value)) {
                                result.add(Boolean.valueOf(value));
                                break;
                            } else {
                                System.err.println("Expect " + value + " is a boolean: true/false but got " + value);
                                System.err.println("ERROR");
                                return null;
                            }
                        }
                    case "3":
                        if (value.contains("\"")) {
                            System.err.println("Expect " + value + " is an integer but got a string");
                            System.err.println("ERROR");
                            return null;
                        } else {
                            if (isInteger(value)) {
                                result.add(Integer.valueOf(value));
                                break;
                            } else {
                                System.err.println("Expect " + value + " is an integer but got " + value);
                                System.err.println("ERROR");
                                return null;
                            }
                        }
                    case "4":
                        if (value.contains("\"")) {
                            System.err.println("Expect " + value + " is an integer but got a string");
                            System.err.println("ERROR");
                            return null;
                        } else {
                            if (isDouble(value)) {
                                result.add(Double.valueOf(value));
                                break;
                            } else {
                                System.err.println("Expect " + value + " is a double but got " + value);
                                System.err.println("ERROR");
                                return null;
                            }
                        }
                    case "5":
                    case "6":
                        if (value.charAt(0) == '"' && (value.substring(value.length()-2)).equals(" \"")){
                            String length = attributeList.get(i).substring(1);
                            value = removeFirstAndLastChar(value);
                            value = value.trim().replaceAll("\\s+$", "");
                            if (value.length() > Integer.parseInt(length)) {
                                System.err.println("Expect " + value + " is a string of max size " + length + " but got " + value.length());
                                System.err.println("ERROR");
                                return null;
                            } else {
                                result.add(value);
                                break;
                            }
                        } else {
                            System.err.println("Expect " + value + " is a string but got " + value + " that is not in quotations");
                            System.err.println("ERROR");
                            return null;

                        }
                }
            }
        }

        return new Record(result, attributeList);
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
                System.out.print("  |  " + attributeName + "  |  ");
            }
            System.out.println("\n\n");
        }

        for (int i = 0; i < pageid_list.size(); i++) {
            Page page = findPageInBuffer(table.getPageID_list().get(i), this);
            if (page == null) {
                int pageID = pageid_list.get(i);
                ArrayList<Integer> pageIDListFromDiskWithRA = getPageIDListFromDiskWithRA(table);
                int indexOfPageID = pageIDListFromDiskWithRA.indexOf(pageID);
                int offset = indexOfPageID * this.page_size;
                page = readFromDiskWithRandomAccess(pageid_list.get(i), table.getTableName(), offset , this.page_size);
                if (page.getRecordList() != null) {
                    if (page.getRecordList().size() == 0) {
                        System.out.println("No record in table: " + table.getTableName());
                        System.out.println("SUCCESS");
                    } else {
                        for (String attributeName : table.getAttriName_list()) {
                            System.out.print("  |  " + attributeName + "  |  ");
                        }
                        System.out.println("\n");
                        for (Record record : page.getRecordList()) {
                            System.out.println(printRecord(record));
                        }
                    }
                } else {
                    for (String attributeName : table.getAttriName_list()) {
                        System.out.print("  |  " + attributeName + "  |  ");
                    }
                    System.out.println("\n");
                }

            } else {
                if (page.getRecordList() != null) {
                    if (page.getRecordList().size() == 0) {
                        System.out.println("No record in table: " + table.getTableName());
                        System.out.println("SUCCESS");
                    } else {
                        for (String attributeName : table.getAttriName_list()) {
                            System.out.print("  |  " + attributeName + "  |  ");
                        }
                        System.out.println("\n");

                        for (Record record : page.getRecordList()) {
                            System.out.println(printRecord(record));
                        }
                    }
                } else {
                    for (String attributeName : table.getAttriName_list()) {
                        System.out.print("  |  " + attributeName + "  |  ");
                    }
                    System.out.println("\n");
                }
            }
        }
    }


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
            } else {
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

    //  enum: {boolean 2, integer 3, double 4, char 5, varchar 6} AttributeType
    //  enum: {primary 0 1}

    /**
     * Method returns the string of a table
     * @param table table
     * @return string of table
     */
    public String tableToString(Table table) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Table Name: ");
        stringBuilder.append(table.getTableName()).append("\n");
        stringBuilder.append("Table schema: \n");
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
        int numRec = getNumRecord(table);
        table.setRecordNum(numRec);
        stringBuilder.append(table.getRecordNum()).append("\n");

        return stringBuilder.toString();
    }


    public int getNumRecord(Table table) {
        int result = 0;
        ArrayList<Integer> NumRecordList = new ArrayList<>();
        String path = this.db_loc + "/Tables/" + table.getTableName() + ".txt";
        File file = new File(path);
        if (file.exists()) {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
                int offset = Integer.BYTES;

                for (int i = 0; i < table.getPageID_list().size(); i++) {
                    randomAccessFile.seek(offset);
                    byte[] numRecArr = new byte[Integer.BYTES];
                    randomAccessFile.readFully(numRecArr);
                    int numRec = ByteBuffer.wrap(numRecArr).getInt();
                    NumRecordList.add(numRec);
                    offset = offset + this.page_size;
                }
            } catch (IOException e) {
                System.err.println("An error occurred while reading from the file.");
                e.printStackTrace();
                System.err.println("ERROR");

            }
        } else {

        }

        for(Integer num : NumRecordList) {
            result = result + num;
        }
        return result;
    }


    public ArrayList<Integer> getPageIDOfWholeCatalog() {
        ArrayList<Integer> pageIDList = new ArrayList<>();
        String path = this.db_loc + "/Tables";
        File directory = new File(path);
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    try {
                        Table table = getCatalog().getTableByName(file.getName().substring(0,3));
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file.getPath(), "rw");
                        int offset = 0;
                        for (int i = 0; i < table.getPageID_list().size(); i++) {
                            randomAccessFile.seek(offset);
                            byte[] pageIDArr = new byte[Integer.BYTES];
                            randomAccessFile.readFully(pageIDArr);
                            int pageID = ByteBuffer.wrap(pageIDArr).getInt();
                            pageIDList.add(pageID);
                            offset = offset + this.page_size;
                        }
                    } catch (IOException e) {
                        System.err.println("An error occurred while reading from the file.");
                        e.printStackTrace();
                        System.err.println("ERROR");
                        return null;
                    }
                }
            }
        }
        return pageIDList;
    }


    /**
     * Method converts arraylist of record string to arraylist of records
     *
     * @param recordList arraylist of record string
     * @param table      table that records belong to
     * @return arraylist of records
     */
//    public ArrayList<Record> convertStringToRecordList(ArrayList<String[]> recordList, Table table) {
//        ArrayList<Record> recordArrayList = new ArrayList<>();
//        ArrayList<String> attriNameList = table.getAttriName_list();
//
//        for (String[] stringarry : recordList) {
//            if (stringarry.length != attriNameList.size()) {
//                System.err.println("Number of attributes don't match. Expect " + attriNameList.size() + " attributes");
//                System.err.println("ERROR");
//                return null;
//            } else {
//                Record record = convertStringArrToRecord(stringarry, table);
//                if (record == null) {
//                    return recordArrayList;
//                }
//                recordArrayList.add(record);
//            }
//        }
//        return recordArrayList;
//    }


    /**
     * Method converts byte array of record to record object
     *
     * @param strArr byte array of record
     * @param table  table
     * @return record object
     */
//    public Record convertStringArrToRecord(String[] strArr, Table table) {
//        ArrayList<Object> valuesList = new ArrayList<>();
//        String primaryKey = table.getPrimaryKeyName();
//        ArrayList<String> attriTypeList = table.getAttriType_list();
//
//        //check primaryKey if duplication and can't be null
//        int indexOfPrimaryKey = getIndexOfColumn(primaryKey, table);
//        for (int i = 0; i < strArr.length; i++) {
//            char attrType = attriTypeList.get(i).charAt(0);
//            if (i == indexOfPrimaryKey) {
//                String value = strArr[i];
//                if (value.equalsIgnoreCase("null")) {
//                    System.err.println("Primary key can't be null");
//                    System.err.println("ERROR");
//                    return null;
//                }
//            }
//            if (attrType == '2') {
//                if (isBoolean(strArr[i])) {
//                    Object valueObj = strArr[i].equalsIgnoreCase("true") ? Boolean.TRUE :
//                            strArr[i].equalsIgnoreCase("false") ? Boolean.FALSE : null;
//                    valuesList.add(valueObj);
//                } else {
//                    System.err.println("Expect " + strArr[i] + " is a boolean: true/false but got " + strArr[i]);
//                    System.err.println("ERROR");
//                    return null;
//                }
//            } else if (attrType == '3') {
//                if (isInteger(strArr[i])) {
//                    Object valueObj = Integer.valueOf(strArr[i]);
//                    valuesList.add(valueObj);
//                } else {
//                    System.err.println("Expect " + strArr[i] + " is a integer but got " + strArr[i]);
//                    System.err.println("ERROR");
//                    return null;
//                }
//            } else if (attrType == '4') {
//                if (isDouble(strArr[i])) {
//                    Object valueObj = Double.valueOf(strArr[i]);
//                    valuesList.add(valueObj);
//                } else {
//                    System.err.println("Expect " + strArr[i] + " is a double but got " + strArr[i]);
//                    System.err.println("ERROR");
//                    return null;
//                }
//            } else if (attrType == '5') {
//                if (!strArr[i].substring(0, 1).equals("\"") || !strArr[i].substring(strArr[i].length() - 1, strArr[i].length()).equals("\"")) {
//                    System.err.println("Expect quotations for string values");
//                    System.err.println("ERROR");
//                    return null;
//                }
//                strArr[i] = strArr[i].substring(1, strArr[i].length() - 1);
//
//                String sizeString = attriTypeList.get(i).substring(1);
//                if (isInteger(sizeString)) {
//                    int size = Integer.parseInt(sizeString);
//                    int inputSize = strArr[i].length();
//                    if (inputSize != size) {
//                        System.err.println("Expect " + strArr[i] + " has " + size + " chars but got " + inputSize + " chars");
//                        System.err.println("ERROR");
//                        return null;
//                    } else {
//                        valuesList.add(strArr[i]);
//                    }
//                }
//            } else if (attrType == '6') {
//                if (!strArr[i].substring(0, 1).equals("\"") || !strArr[i].substring(strArr[i].length() - 1, strArr[i].length()).equals("\"")) {
//                    System.err.println("Expect quotations for string values");
//                    System.err.println("ERROR");
//                    return null;
//                }
//                // take out the quote in the front and at the end.
//                strArr[i] = strArr[i].substring(1, strArr[i].length() - 1);
//
//                String sizeString = attriTypeList.get(i).substring(1);
//                if (isInteger(sizeString)) {
//                    int size = Integer.parseInt(sizeString);
//                    int inputSize = strArr[i].length();
//                    if (inputSize > size) {
//                        System.err.println("Expect " + strArr[i] + " has " + size + " chars but got " + inputSize + " chars");
//                        System.err.println("ERROR");
//                        return null;
//                    } else {
//                        valuesList.add(strArr[i]);
//                    }
//                }
//            } else {
//                System.err.println("Fail to parse the input to record");
//                System.err.println("ERROR");
//                return null;
//            }
//        }
//        return new Record(valuesList, new ArrayList<String>());
//    }



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
//    public ArrayList<Record> returnListofStringArrRecord(String inputTupesList, Table table) {
//        ArrayList<Record> recordList = new ArrayList<>();
//        String[] splitString = inputTupesList.split(",");
//
//        for (String str : splitString) {
//            if (!str.substring(0, 1).equals("(") || !str.substring(str.length() - 1, str.length()).equals(")")) {
//                return null;
//            }
//            String tupleAfterRemoveParenthesis = removeFirstAndLastChar(str);
//            String[] tupleSplitBySpace = tupleAfterRemoveParenthesis.split(" ");
//            if (tupleSplitBySpace.length != table.getAttriName_list().size()) {
//                // number of attributes don't match
//                return null;
//            }
//
//            Record r = convertStringArrToRecord(tupleSplitBySpace, table);
//            if (r != null) {
//                recordList.add(r);
//            }
//        }
//        return recordList;
//    }


    /**
     * Method inserts the record to table
     *
     * @param inp       input SQL
     * @param tablename table name
     * @return true if successfully otherwise false
     */
//    public boolean insertRecordToTable(String inp, String tablename) {
//        if (this.storageManager.getCatalog().getTablesList() != null) {
//            Table table = this.storageManager.getTableByName(tablename);
//            if (table != null) {
//                int indexOfPrimaryKey = getIndexOfColumn(table.getPrimaryKeyName(), table);
//                int startIndex = inp.indexOf("(");
//                inp = inp.substring(startIndex, inp.length() - 1);
//                ArrayList<Record> recordArrayList = returnListofStringArrRecord(inp, table);
//                //ArrayList<Main.Record> recordArrayList = convertStringToRecordList(stringArrRecordList, table);
//                if (recordArrayList != null) {
//                    for (Record record : recordArrayList) {
//                        boolean insertAlready = false;
//                        if (table.getPageID_list().size() == 0) {
//                            removeLRUFromBufferIfOverflow(getCatalog());
//                            Page page = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
//                            page.getRecordList().add(record);
//                            page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
//                            table.getPageID_list().add(getPageIDList().size() + 1);
//                            table.increaseNumRecordBy1();
//
//                            // change split
//                            if (record.convertRecordToByteArr(record).length > this.page_size) {
//                                System.out.println("page size" + this.page_size);
//                                System.err.println("This record is bigger than page size");
//                                return false;
//                            }
//
//                            //check if the page is in the buffer
//                            // if not then add to pagelist buffer
//                            if (!this.pagelistBuffer.contains(page)) {
//                                this.pagelistBuffer.add(page);
//                                removeLRUFromBufferIfOverflow(getCatalog());
//                                //else then remove that page and add back to the last (this is the most recently use)
//                            } else {
//                                int indexPage = this.pagelistBuffer.indexOf(page);
//                                if (indexPage != -1) {
//                                    this.pagelistBuffer.remove(indexPage);
//                                    this.pagelistBuffer.add(page);
//                                    removeLRUFromBufferIfOverflow(getCatalog());
//                                }
//
//                            }
//                            insertAlready = true;
//                        } else {
//                            ArrayList<Integer> originalPageIDList = new ArrayList<>(table.getPageID_list());
//
//                            for (int m = 0; m < originalPageIDList.size(); m++) {
//
//                                Page page = new Page(originalPageIDList.get(m), table.getTableName(), this.db_loc);
//                                if (!this.pagelistBuffer.contains(page)) {
//                                    this.pagelistBuffer.add(page);
//                                    removeLRUFromBufferIfOverflow(getCatalog());
//                                } else {
//                                    for (Page tempPage : this.pagelistBuffer) {
//                                        if (tempPage.equals(page)) {
//                                            page = tempPage;
//                                            break;
//                                        }
//                                    }
//                                    int indexPage = this.pagelistBuffer.indexOf(page);
//                                    if (indexPage != -1) {
//                                        this.pagelistBuffer.remove(indexPage);
//                                        this.pagelistBuffer.add(page);
//                                        removeLRUFromBufferIfOverflow(getCatalog());
//                                    }
//                                }
//
//                                for (int i = 0; i < page.getRecordList().size(); i++) {
//                                    Record tempRecord = page.getRecordList().get(i);
//                                    Object recordPrimaryValue = record.getValuesList().get(indexOfPrimaryKey);
//                                    Object tempRecordPrimaryValue = tempRecord.getValuesList().get(indexOfPrimaryKey);
//                                    int compare = compare2Records(recordPrimaryValue, tempRecordPrimaryValue);
//                                    if (compare == 0) {
//                                        System.err.println("Can't have duplicated primary key: " + printRecord(record));
//                                        System.err.println("ERROR");
//                                        return false;
//                                    }
//
//                                    // if primary key of new record does not duplicate
//                                    // split page
//                                    if (compare < 0) {
//                                        System.out.println(this.page_size);
//                                        page.getRecordList().add(i, record);
//                                        page.incCurrentPageSize(record, record.convertRecordToByteArr(record));
//                                        table.increaseNumRecordBy1();
//                                        int currentPagesize = page.getCurrent_page_size();
//                                        //check if page overflows
//                                        if (currentPagesize > this.page_size) {
//                                            int midpoint = page.getRecordList().size() / 2;
//                                            ArrayList<Record> halfRecord = new ArrayList<>();
//                                            // haldRecord stores the second half of the page when it overflows
//                                            for (int j = midpoint; j < page.getRecordList().size(); j++) {
//                                                halfRecord.add(page.getRecordList().get(j));
//                                            }
//
//                                            page.getRecordList().subList(midpoint, page.getRecordList().size()).clear();
//                                            Page newPage = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
//                                            newPage.getRecordList().addAll(halfRecord);
//
//                                            //decrement page 1 size after splitting
//                                            //increment those into page 2
//                                            for (Record rec : halfRecord) {
//                                                page.decCurrentPageSize(rec, record.convertRecordToByteArr(rec));
//                                                newPage.incCurrentPageSize(rec, record.convertRecordToByteArr(rec));
//                                            }
//
//                                            //add new page into buffer
//                                            this.pagelistBuffer.add(newPage);
//                                            //check if buffer full
//                                            removeLRUFromBufferIfOverflow(getCatalog());
//                                            ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
//                                            //add the new page into the table in catalog
//                                            for (Table tbl : tableListInCatalog) {
//                                                if (tbl.getTableName().equals(table.getTableName())) {
//                                                    tbl.getPageID_list().add(m + 1, newPage.getPageID());
//                                                }
//                                            }
//                                        }
//                                        insertAlready = true;
//                                        break;
//                                    }
//                                }
//                            }
//                        }
//                        if (!insertAlready) {
//                            ArrayList<Integer> pageIDList = table.getPageID_list();
//                            Page lastPage = new Page(pageIDList.get(pageIDList.size() - 1), table.getTableName(), this.db_loc);
//                            for (Page tempPage : this.pagelistBuffer) {
//                                if (tempPage.equals(lastPage)) {
//                                    lastPage = tempPage;
//                                    break;
//                                }
//                            }
//                            lastPage.getRecordList().add(record);
//                            lastPage.incCurrentPageSize(record, record.convertRecordToByteArr(record));
//                            table.increaseNumRecordBy1();
//                            int currentPagesize = lastPage.getCurrent_page_size();
//                            if (currentPagesize > this.page_size) {
//                                int midpoint = lastPage.getRecordList().size() / 2;
//                                ArrayList<Record> halfRecord = new ArrayList<>();
//                                for (int j = midpoint; j < lastPage.getRecordList().size(); j++) {
//                                    halfRecord.add(lastPage.getRecordList().get(j));
//                                }
//                                lastPage.getRecordList().subList(midpoint, lastPage.getRecordList().size()).clear();
//                                Page newPage = new Page(getPageIDList().size() + 1, table.getTableName(), this.db_loc);
//                                newPage.getRecordList().addAll(halfRecord);
//                                for (Record rec : halfRecord) {
//                                    lastPage.decCurrentPageSize(rec, record.convertRecordToByteArr(rec));
//                                    newPage.incCurrentPageSize(rec, record.convertRecordToByteArr(rec));
//                                }
//
//                                this.pagelistBuffer.add(newPage);
//                                removeLRUFromBufferIfOverflow(getCatalog());
//                                ArrayList<Table> tableListInCatalog = this.storageManager.getCatalog().getTablesList();
//                                for (Table tbl : tableListInCatalog) {
//                                    if (tbl.getTableName().equals(table.getTableName())) {
//                                        tbl.getPageID_list().add(newPage.getPageID());
//                                        break;
//                                        //tbl.getPageID_list().add(tableListInCatalog.size() + 1, newPage.getPageID());
//                                    }
//                                }
//                            }
//                        }
//                    }
//
//                    if (inp.split(",").length == recordArrayList.size()) {
//                        System.out.println("All record inserted.");
//                        return true;
//                    } else if (recordArrayList.size() > 0) {
//                        System.out.println("The first " + recordArrayList.size() + " record has been inserted.");
//                        return false;
//                    } else {
//                        return false;
//                    }
//
//                }
//            }
//            return false;
//        }
//        return false;
//    }


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

}


