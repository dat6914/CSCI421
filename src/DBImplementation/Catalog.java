package DBImplementation;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Catalog {
    ArrayList<Table> tables_list = new ArrayList<>();

    HashMap<String, Table> tableMap= new HashMap<>();

    HashMap<String, ArrayList<String>> tableTypeMap = new HashMap<>();

    public Catalog() {
    }

    public void insertTable(Table table){
        tableMap.put(table.getTableName(), table);
    }

    public void insertTypeMap(String tablename, ArrayList<String> tableTypeList){
        tableTypeMap.put(tablename, tableTypeList);
    }

    public ArrayList<Table> getTables_list(){
        return tables_list;
    }

    public HashMap<String, Table> getTableMap(){
        return tableMap;
    }
    




}
