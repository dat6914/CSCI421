import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
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
                displaySchema();
            } else if (input.equals("<quit>")) {
                quitProgram();
                break;
            } else if (optionArr[0].equals("display") && optionArr[1].equals("info") && optionArr.length == 3) {
                displayInfo(optionArr[2]);
            } else if (optionArr[0].equals("insert") && optionArr[1].equals("info") && optionArr[3].equals("values") && optionArr.length >= 5) {
                //Tuple = parse string aaaaa
                insertTuplesIntoTable(optionArr[2], optionArr[4]);
            } else if (optionArr[0].equals("select") && optionArr[1].equals("*") && optionArr[2].equals("from") && optionArr.length == 4) {
                selectRecordsFromTable(optionArr[3]);
            } else if (optionArr[0].equals("create") && optionArr[1].equals("tables") && optionArr.length > 3) {
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

    public static void displayInfo(String tableName) {
        System.out.println("Table name: ");
        System.out.println("Table schema: ");
        System.out.println("Number of pages: ");
        System.out.println("Number of records: ");
    }

    public static void displaySchema() {
        System.out.println("Database location: " );
        System.out.println("Page size: ");
        System.out.println("Buffer size: ");
        System.out.println("Table schema: ");
    }

    public static void quitProgram() {
        //terminate the database
        System.out.println("Quit program safely!");
    }

    public static void displayCommand() {
        System.out.println("----------------------------------------------");
        System.out.println("Storage Manager!");
        System.out.println("List of commands:");
        System.out.println("    display schema");
        System.out.println("    display info <name>");
        System.out.println("    select * from <name>");
        System.out.println("    insert into <name> values <tuples>");
        System.out.println("    create table <name> (");
        System.out.println("        <attr_name1> <attr_type1> primarykey,");
        System.out.println("        <attr_name2> <attr_type2>,...");
        System.out.println("        <attr_nameN> <attr_typeN>  );");
        System.out.println("    quit program");

    }
}
