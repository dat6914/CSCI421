import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

//This is table page
public class Table {
    private String tableName;
    private String primaryKeyName;
    private ArrayList<String> attriName_list;
    private ArrayList<String> attriType_list;
    private ArrayList<Integer> pageID_list = new ArrayList<>();
    private int recordNum = 0; //TODO

    public Table(String tableName, String primaryKeyName, ArrayList<String> attriNameList, ArrayList<String> attriTypeList) {
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.attriName_list = attriNameList;
        this.attriType_list = attriTypeList;
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

    /**
     * Method gets number of records
     * @return number of records
     */
    public int getRecordNum() {
        return recordNum;
    }






}
