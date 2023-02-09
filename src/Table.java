import java.util.ArrayList;

//This is table page
public class Table {
    String tableName;
    private String primaryKeyName;
    private ArrayList<String> attriName_list = new ArrayList<>();
    private ArrayList<String> attriType_list = new ArrayList<>();
    private ArrayList<Record> record_list = new ArrayList<>();
    private ArrayList<Page> pages_list = new ArrayList<>();

    private ArrayList<Page> page_list = new ArrayList<>();

    public Table(String tableName, String primaryKeyName, ArrayList<String> attriNameList, ArrayList<String> attriTypeList) {
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.attriName_list = attriNameList;
        this.attriType_list = attriTypeList;
        this.page_list = page_list;
        this.tableName = tableName;
    }
    
    

    public Record getRecordByPrimaryKey(String primaryKeyName) {
        return null;
    }

    public ArrayList<Record> getRecord_list() {
        return record_list;
    }

    public boolean insertRecordToTable(Record record) {
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

    public String getAttrType(int index) {
        return attriType_list.get(index);
    }

    public String getSchema() {
        return null;
    }

    public ArrayList<Page> getPage_list() {
        return page_list;
    }
}
