package DBImplementation;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Main {

    public static Catalog catalog = new Catalog();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        if (args.length != 3) {
            System.err.println("Usage: <db_loc> <page_size> <buffer_size>");
        }
        String db_loc = args[0];
        int page_size = Integer.parseInt(args[1]);
        int buffer_size = Integer.parseInt(args[2]);


        //Check if there is database at the given database location
        //if there is then restart that database
        //else, create a new database at given location with pagesize and buffersize

        File directory = new File(String.valueOf(db_loc));
        System.out.println("Searching for database at " + db_loc + "...");
        if (!directory.exists()) {
            System.out.println("Database does not exist. Creating a new database at " + db_loc + "...");
            directory.mkdir();
        } else {
            System.out.println("Database exists.");
        }
        System.out.println("Page Size: " + page_size);
        System.out.println("Buffer Size: " + buffer_size);

        StorageManager storageManager = new StorageManager(db_loc, page_size, buffer_size);

        Database database = null;

        System.out.println("Database now running...");
        System.out.println("Please enter commands, enter <quit> to shutdown the db.");

        while(true) {
            displayCommand();
            System.out.println("Enter the command: ");
            String input = scanner.nextLine();
            String[] optionArr = input.split(" ");
            if (input.equals("display schema")) {
                displaySchema(db_loc, page_size, buffer_size);
            } else if (input.equals("<quit>")) {
                quitProgram();
                break;
            } else if (optionArr[0].equals("display") && optionArr[1].equals("info") && optionArr.length == 3) {
                displayInfo(optionArr[2]);
            } else if (optionArr[0].equals("insert") && optionArr[1].equals("into") && optionArr[3].equals("values") && optionArr.length >= 5) {
                if (tableExist(optionArr[2])){
                    insertRecordIntoTable(optionArr[2], optionArr[4]);
                }else{
                    System.err.println("Invalid Table Name" + optionArr[2]);
                    displayCommand();
                }


                //insertTuplesIntoTable(optionArr[2], optionArr[4]);


            } else if (optionArr[0].equals("select") && optionArr[1].equals("*") && optionArr[2].equals("from") && optionArr.length == 4) {
                selectRecordsFromTable(optionArr[3]);
            } else if (optionArr[0].equals("create") && optionArr[1].equals("table") && optionArr.length > 3) {
                //Tuple = parse string
                createTable(optionArr[2], optionArr[3]);
            } else {
                System.err.println("It is not a valid command.");
            }
        }


    }

    public static void createTable(String tableName, String tuples) {

    }

    public static void selectRecordsFromTable(String tableName) {

    }

    public static void insertTuplesIntoTable(String tableName, String tuples) {

    }

    public static void insertRecordIntoTable(String tableName, String valTuple){
        if (pageExist(tableName)){
            // insert curr record into existing page.
        }else{
            // create new page and record curr record.
        }
    }


    public static int parseChar(String c){
        StringBuilder builder = new StringBuilder();
        int temp = 0;
        for(int i = 5; i <= c.length(); i++){
            if(c.charAt(i) != ']'){
                builder.append(c.charAt(i));
            }else{
                temp = Integer.parseInt(builder.toString());
            }
        }
        return temp;
    }

    public static int parseVarchar(String c){
        StringBuilder builder = new StringBuilder();
        int temp = 0;
        for(int i = 8; i <=c.length(); i++){
            if(c.charAt(i) != ']'){
                builder.append(c.charAt(i));
            }else{
                temp = Integer.parseInt(builder.toString());
            }
        }
        return temp;
    }

    public ArrayList<Object> parseValue(String tableName, String value){
        ArrayList<Object> h = new ArrayList<>();
        String[] valueArr = value.split(",");
        //
        for(String i : valueArr){


        }
        return h;

    }

    public static boolean pageExist(String tableName){
        HashMap<String,Table> tableMap = catalog.getTableMap();
        Table table = tableMap.get(tableName);
        if(table.getPage_list().size() == 0){
            return false;
        }else{
            return true;
        }

    }

    public static boolean tableExist(String tablename){
        return catalog.getTableMap().containsKey(tablename);
    }



    public static void displayInfo(String tableName) {
        System.out.println("Table name: " + tableName);
        System.out.println("Table schema: ");
        System.out.println("Number of pages: ");
        System.out.println("Number of records: ");
    }

    public static void displaySchema(String location, int pageSize, int bufferSize) {
        System.out.println("DB location: " + location);
        System.out.println("Page size: " + pageSize);
        System.out.println("Buffer size: " + bufferSize);
        System.out.println("\n ");
        //todo: create function for checking for table detail that will take the param of location
        //todo: check if it's successful
    }

    public static void quitProgram() {
        //terminate the database
        //todo: call pagebuffer to write pages to hardware before shut down
        //todo: catalog to save all data
        System.out.println("Safely shutting down the database...");
        System.out.println("Purging page buffer...");
        System.out.println("Saving catalog...");
    }

    public static void displayCommand() {
        System.out.println("----------------------------------------------");
        System.out.println("Storage Manager!");
        System.out.println("List of commands:");
        System.out.println("    1. display schema");
        System.out.println("    2. display info <name>");
        System.out.println("    3. select * from <name>");
        System.out.println("    4. insert into <name> values <tuples>");
        System.out.println("    5. create table <name>(");
        System.out.println("          <attr_name1> <attr_type1> primarykey,");
        System.out.println("          <attr_name2> <attr_type2>,...");
        System.out.println("          <attr_nameN> <attr_typeN>);");
        System.out.println("    6. <quit>");

    }
}
