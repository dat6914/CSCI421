package Main;

import java.util.ArrayList;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This class is to hold information about the table
 */
public class Table {
    private String tableName;
    private String primaryKeyName;
    private ArrayList<String> attriName_list;
    private ArrayList<String> attriType_list;
    private ArrayList<Integer> pageID_list;
    private int recordNum = 0;
    private int pageNum = 0;
    private String db_loc;


    public Table(String tableName, String primaryKeyName, ArrayList<String> attriNameList, ArrayList<String> attriTypeList, String db_loc, ArrayList<Integer> pageID_list) {
        this.tableName = tableName;
        this.primaryKeyName = primaryKeyName;
        this.attriName_list = attriNameList;
        this.attriType_list = attriTypeList;
        this.db_loc = db_loc;
        this.pageID_list = pageID_list;

    }

    /**
     * Method increases the number of record by 1
     */
    public void increaseNumRecordBy1() {
        this.recordNum = this.recordNum + 1;
    }

    public void incrementPageNum(){
        this.pageNum += 1;
    }

    /**
     * Method get the number of record after updated
     * @return the number of records
     */
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

    public byte[] serializeTable(Page page, Table table){


        return new byte[10];

    }

}
