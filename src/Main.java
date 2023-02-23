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

        File directory = new File(String.valueOf(db_loc));
        System.out.println("Searching for database at " + db_loc + "...");
        if (!directory.exists()) {
            System.out.println("Database does not exist. \nCreating a new database at " + db_loc + "...");
            directory.mkdir();
        } else {
            System.out.println("Database exists.");
        }

        System.out.println("Page Size: " + page_size);
        System.out.println("Buffer Size: " + buffer_size);

        StorageManager storageManager = new StorageManager(db_loc, page_size, buffer_size);
        PageBuffer pageBuffer = new PageBuffer(db_loc, page_size, buffer_size);

        Catalog catalog = storageManager.getCatalog();
        System.out.println("----------------------------------------------");
        System.out.println("Storage Manager!");
        System.out.println("Database's now running...");
        displayCommand();

        while(true) {
            System.out.println("Please enter commands, enter <quit> to shutdown the database > ");
            String input = scanner.nextLine();
            String[] optionArr = input.split(" ");
            if (input.equals("display schema;")) {
                catalog.displaySchema(args[0], page_size, buffer_size);

            } else if (optionArr[0].equals("display") && optionArr[1].equals("info") && optionArr.length == 3) {
                optionArr[2] = optionArr[2].substring(0, optionArr[2].length() - 1);
                catalog.displayInfoTable(optionArr[2]);

            } else if (optionArr[0].equals("create") && optionArr[1].equals("table") && optionArr.length > 3) {
                if (catalog.createTable(input) != null) {
                    System.out.println("SUCCESS");
                }
            } else if (input.equals("<quit>")) {
                if (pageBuffer.quitProgram()) {
                    System.out.println("SUCCESS");
                } else {
                    System.err.println("ERROR");
                }
                break;
            } else if (optionArr[0].equals("insert") && optionArr[1].equals("into") && optionArr[3].equals("values") && optionArr.length >= 5) {
                if (pageBuffer.insertRecordToTable(input, optionArr[2])) {
                    System.out.println("SUCCESS");
                } else {
                    System.err.println("ERROR");
                }

            } else if (optionArr[0].equals("select") && optionArr[1].equals("*") && optionArr[2].equals("from") && optionArr.length == 4) {
                storageManager.selectStarFromTable(optionArr[3].substring(0, optionArr[3].length()-1));

            } else if (optionArr[0].equals("get") && optionArr[1].equals("page") && optionArr[3].equals("in") &&
                    optionArr[5].equals("table") && optionArr.length==6) {
                if (isInteger(optionArr[2])) {
                    if (storageManager.getPageByTableAndPageNumber(optionArr[5], Integer.parseInt(optionArr[2]))) {
                        System.out.println("SUCCESS");
                    } else {
                        System.err.println("ERROR");
                    }
                } else {
                    System.err.println("pageID has to be a number!");
                    System.err.println("ERROR");
                }

            } else if (optionArr[0].equals("select") && optionArr[1].equals("record") && optionArr[2].equals("from") &&
                    optionArr[4].equals("where") && optionArr[5].equals("primarykey") && optionArr.length == 7) {
                if (storageManager.getRecordByPrimaryKey(optionArr[6], optionArr[3]) != null) {
                    System.out.println("SUCCESS");
                } else {
                    System.err.println("ERROR");
                }
            } else {
                System.err.println("It is not a valid command.");

            }
        }
    }

    public static void displayCommand() {
        System.out.println("List of commands:");
        System.out.println("    display schema");
        System.out.println("    display info <name>;");
        System.out.println("    select * from <name>");
        System.out.println("    insert into <name> values <tuples>");
        System.out.println("    create table <name> (");
        System.out.println("        <attr_name1> <attr_type1> primarykey,");
        System.out.println("        <attr_name2> <attr_type2>,...");
        System.out.println("        <attr_nameN> <attr_typeN> );");
        System.out.println("    get page <pageID> in table <name>" );
        System.out.println("    select record from <tableName> where primarykey <primarykey_value>");
    }

    /**
     * This method checks if a string is an integer or not
     * @param str string needs to be checks
     * @return true if a string is integer, false if not
     */
    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
