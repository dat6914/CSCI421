import java.nio.ByteBuffer;
import java.util.ArrayList;


//This is table page
public class Table {
    private String tableName;
    private String primaryKeyName;
    private ArrayList<String> attriName_list;
    private ArrayList<String> attriType_list;
    private ArrayList<Integer> pageID_list;
    private int recordNum = 0;
    private String db_loc;


    public Table(String tableName, String primaryKeyName, ArrayList<String> attriNameList, ArrayList<String> attriTypeList, String db_loc, ArrayList<Integer> pageID_list) {
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.attriName_list = attriNameList;
        this.attriType_list = attriTypeList;
        this.db_loc = db_loc;
        this.pageID_list = pageID_list;
        this.recordNum = getRecordNum(pageID_list);

    }

    public int getRecordNum (ArrayList<Integer> pageID_list) {
        int result = 0;

        for (int i = 0; i < pageID_list.size(); i++) {
            int index = pageID_list.get(i);
            Page page = new Page(index, this, this.db_loc);
            result =  result + page.getRecordList().size();
        }
        return result;
    }

    public void increaseNumRecordBy1() {
        this.recordNum = this.recordNum + 1;
    }

    public int getRecordNumUpdate() {
        return this.recordNum;
    }


    /**
     * Method gets pageID list
     * @return arraylist of pageID
     */
    public ArrayList<Integer> getPageID_list() {
        return pageID_list;
    }

    /**
     * Method gets an attribute type at an given index
     * @param index index of attribute type
     * @return attribute type
     */
    public String getAttrType(int index) {
        return attriType_list.get(index);
    }

    /**
     * Method gets table name
     * @return tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Method gets arraylist of attribute names
     * @return arraylist of attribute names
     */
    public ArrayList<String> getAttriName_list() {
        return attriName_list;
    }

    /**
     * Method gets arraylist of attribute types
     * @return arraylist of attribute types
     */
    public ArrayList<String> getAttriType_list() {
        return attriType_list;
    }

    /**
     * Method gets Name of primarykey
     * @return Name of primarykey
     */
    public String getPrimaryKeyName() {
        return primaryKeyName;
    }







}