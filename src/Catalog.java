import java.util.ArrayList;

public class Catalog {

    ArrayList<Table> tables_list = new ArrayList<>();


    public Catalog() {

    }


    /*
     * Most of these functions are gonna be used by the query processor and since this is the catalog class,
     * make sure we're not touching ANY DATA HERE. Data has to be stored in binary on hardware and must be saved between runs.
     */




    public void createTable(String tableName, String primaryKeyName, ArrayList<String> attriName_list, ArrayList<String> attriType_list) {
        // check if the table already exists
        for (Table table : tables_list) {
            if (table.tableName.equals(tableName)) {
                System.out.println("ERROR: Table already exists.");
                return;
            }
        }

        //check if primary key is in the list of attributes
        boolean isPrimaryKeyInAttriList = false;
        for (String attriName : attriName_list) {
            if (attriName.equals(primaryKeyName)) {
                isPrimaryKeyInAttriList = true;
                break;
            }
        }

        //check if elements in attriName_list are unique, case-insensitive
        for (int i = 0; i < attriName_list.size(); i++) {
            for (int j = i + 1; j < attriName_list.size(); j++) {
                if (attriName_list.get(i).equalsIgnoreCase(attriName_list.get(j))) {
                    System.out.println("ERROR: Attribute names are not unique.");
                    return;
                }
            }
        }

        Table table = new Table(tableName, primaryKeyName, attriName_list, attriType_list);

        tables_list.add(table);
        System.out.println("SUCCESS");
    }

    public void displayTableInfo(String tableName) {
        for (Table table : tables_list) {
            if (table.tableName.equals(tableName)) {
                System.out.println("Table name: " + table.tableName);
                System.out.println("Table schema: " + table.getSchema());
                System.out.println("Pages: " + table.getPage_list().size());
                System.out.println("Records: " + table.getRecord_list().size());

                System.out.println("\nSUCCESS\n");
                return;
            }
        }
        System.out.println("ERROR: Table does not exist.");
    }


}
