package DBImplementation;

import java.util.ArrayList;

public class Table {
    private String tableName;
    private String primaryKeyName;
    private ArrayList<String> attriName_list = new ArrayList<>();
    private ArrayList<Page> page_list = new ArrayList<>();

    public Table(String tableName,String primaryKeyName, ArrayList<String> attriName_list){
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.attriName_list = attriName_list;
    }



    public Record getRecordByPrimaryKey(String primaryKeyName) {
        return null;
    }

    public String getTableName(){
        return tableName;
    }

    public ArrayList<Page> getPage_list() {
        return page_list;
    }

    public boolean insertRecord(Record record) {
        return false;
    }

    //primary key here is the value not the column name (primaryKeyName)
    //nho di con
    public boolean deleteRecord(String primaryKey) {
        return false;
    }
    public boolean updateRecord(Record record) {
        return false;
    }

    public int getIndexOfColumn(String colName) {
        return -1;
    }

    //primary key here is the value not the column name (primaryKeyName)
    //nho di con
    public boolean updateRecord(String primaryKey, Record record) {
        return false;
    }

    public boolean saveTableInFile(String fileName) {
        return false;
    }

    public static Table readTable(String fileName) {
        return null;
    }


}
