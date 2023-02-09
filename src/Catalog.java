import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

public class Catalog {

    private ArrayList<Table> tables_list = new ArrayList<>();



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
    public byte[] loadSchema(String path)  {
        //load Catalog

        try {

            String input = Files.readString(Path.of(path));

            byte[] content = input.getBytes(StandardCharsets.UTF_8);
            for(int i = 0; i < content.length ; i++) {
                System.out.print(content[i] +" ");
            }

            byte[] table = new byte[4];
            for (int i = 0; i < 4; i++) {
                table[i] = content[i];
            }
            int tableNum = Integer.parseInt(new String(table));



        } catch (IOException e) {
            System.err.println(e.getMessage());
        }


        return null;
    }


    public boolean writeTabletoCatalog (String s) {
        String input = "create table student( name varchar(15), studentID integer primarykey, address char(20), gpa double, incampus boolean)";
        String[] table = input.split("[\\s,]+");
        StringBuilder content = new StringBuilder();

        //remove the ( after tableName and ) at end
        table[2] = table[2].split("\\(")[0];
        table[table.length-1] = table[table.length-1].split("\\)")[0];

        //totalsize of byte[]
        int totalsizeByteArr = 0;

        //get table name and size
        byte[] tableName = table[2].getBytes(StandardCharsets.UTF_8);
        int tableNameSize = tableName.length;
        byte[] nameSize = ByteBuffer.allocate(4).putInt(tableNameSize).array();
        totalsizeByteArr = totalsizeByteArr + 4;
        totalsizeByteArr = totalsizeByteArr + tableNameSize;
        content.append(Arrays.toString(nameSize));
        content.append(Arrays.toString(tableName));

        System.out.println(content.toString());

        int attrNum = (table.length - 3 - 1)/2;
        byte[] attributeNum = ByteBuffer.allocate(4).putInt(attrNum).array();

        // table: name varchar(15) studentID integer primarykey address char(20) gpa double incampus boolean
        for (int i = 3; i < table.length; i++) {

        }


        return false;
    }





}
