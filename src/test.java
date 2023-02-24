import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

public class test {
    public static void main(String[] args) throws IOException {
//        String path = "/Users/daotran/Downloads/database";
//        StorageManager storageManager = new StorageManager(path, 4096, 10);
//        Catalog catalog = storageManager.getCatalog();
//
//
//        String input = "create table student (name varchar(15), studentID integer primarykey, address char(30), gpa double, incampus boolean);";
//        String input2 = "create table course( courseID integer primarykey, name varchar(10) );";
//        Table newTable1 = catalog.createTable(input);
//        Table newTable2 = catalog.createTable(input2);
//        ArrayList<Table> tableArrayList = catalog.getTablesList();
//        String insertInput = "insert into student values (\"Alice\" 1234 \"43 Noel Drive ,Rochester\" 3.2 true) , (\"(A)\" 1 \"(school)\" 2 false);";
//        ArrayList<String[]> arrayListofStringArr = storageManager.returnListofStringArrRecord(insertInput);
//        ArrayList<Record> recordArrayList = storageManager.convertStringToRecordList(arrayListofStringArr, newTable1);
//        storageManager.insertRecordToTable(insertInput, newTable1);
//        storageManager.getPageByTableAndPageNumber(newTable1.getTableName(), newTable1.getPageID_list().get(0));
//        String testDup = "insert into student values (\"Mimi\" 1234 \"DDDDDDrive ,Rochester\" 10 False);";
//        ArrayList<String[]> arrayListofStringArrTestDup = storageManager.returnListofStringArrRecord(testDup);
//        ArrayList<Record> recordArrayListTestDup = storageManager.convertStringToRecordList(arrayListofStringArrTestDup, newTable1);

//        byte[] catalogByte = schema.convertCatalogToByteArr(schema);
//        //schema.saveTableToCatalogAndDisk(path, catalogByte);
//
//        byte[] table1 = schema.convertTableToByteArr(newTable1);
//        byte[] table2 = schema.convertTableToByteArr(newTable2);
//
//        //schema.saveTableToCatalogAndDisk(path, table1);
//        //schema.saveTableToCatalogAndDisk(path, table2);
//
//        byte[] catalogArr = readCatalogFile(path);
//        ArrayList<Table> tableList = schema.convertByteArrToCatalog(catalogArr);
//
//
//
//        //String insertInput = "insert into student values (\"Alice\" 1234 \"43 Noel Drive Rochester\" 3.2 true) , (\"(A)\" 1 \"(school)\" 2 (false))";
//        ArrayList<Record> recordArrayList = new ArrayList<>();
//        ArrayList<Object> valueList = new ArrayList<>();
//        valueList.add("Alice");
//        valueList.add(Integer.valueOf(1234));
//        valueList.add("43 Noel Drive");
//        valueList.add(Double.valueOf(3.2));
//        valueList.add(Boolean.valueOf(false));
//        Record newRecord1 = new Record(valueList);
//        recordArrayList.add(newRecord1);
//
//        ArrayList<Object> valueList1 = new ArrayList<>();
//        valueList1.add("Sakura");
//        valueList1.add(Integer.valueOf(86));
//        valueList1.add("43 Noel Drive");
//        valueList1.add(Double.valueOf(4.0));
//        valueList1.add(Boolean.valueOf(true));
//        Record newRecord2 = new Record(valueList1);
//        recordArrayList.add(newRecord2);
//
//        Page page = new Page(123, newTable1, 300, path);
//        byte[] pageArr = page.convertPageToByteArr(recordArrayList, newTable1, 300);
//        ArrayList<Record> recordTestList = page.convertByteArrToPage(pageArr, newTable1, 300);
//
//
//        ArrayList<Object> primaryValueList = storageManager.getPrimarykeyValueList(newTable1);
//        String pagePath = path + "/Pages/" + page.getPageID() + ".txt";
//        writeByteArrToDisk(pagePath, pageArr);

        //storageManager.sortPrimaryKey(primaryValueList);

//        byte[] recordByteArr = storageManager.(newRecord, newTable1);
//        Record record = newRecord.convertByteArrToRecord(recordByteArr, newTable1);
//
//        Record res = newRecord.convertByteArrToRecord(recordByteArr, newTable1);
//        ArrayList<Record> recordArrayList = schema.checkInsertRecordSQL(insertInput, newTable1);
//        schema.displaySchema(path, 4096, 10);

//        byte[] pageArrTest = readFile(pagePath);
//
//        ArrayList<Record> testing = page.convertByteArrToPage(pageArrTest, newTable1, 300);

//        System.out.println("KKKKK");





    }
//    public static byte[] readFile (String path) {
//        try {
//            File file = new File(path);
//            FileInputStream fs = new FileInputStream(file);
//            byte[] arr = new byte[(int)file.length()];
//            fs.read(arr);
//            fs.close();
//            System.out.println("SUCCESS");
//            return arr;
//        } catch (IOException e) {
//            System.err.println("An error occurred while writing to the file.");
//            e.printStackTrace();
//            System.err.println("ERROR");
//
//        }
//        return null;
//    }
//
//
//
//    //TODO: when the command <quit> then write table to schema and then add to tablelist
//    //for now i just write directly for TESTING
//    public static void writeByteArrToDisk(String path, byte[] data) {
//        File tempFile = new File(path);
//        if (tempFile.exists()) {
//            try {
//                FileOutputStream fs = new FileOutputStream(path, true);
//                fs.write(data);
//                fs.close();
//            } catch (IOException i) {
//                System.err.println("An error occurred while writing to the file.");
//                i.printStackTrace();
//                System.err.println("ERROR");
//            }
//
//        } else {
//            try {
//                FileOutputStream fs = new FileOutputStream(path);
//                fs.write(data);
//                System.out.println("SUCCESS");
//            } catch (IOException e) {
//                System.err.println("An error occurred while writing to the file.");
//                e.printStackTrace();
//                System.err.println("ERROR");
//
//            }
//        }
//    }


}
