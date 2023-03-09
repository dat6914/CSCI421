package Main;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

public class Main {
    public static void main(String[] args) throws IOException {
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
            File directPB = new File(db_loc);
            if (!directPB.exists()) {
                boolean create = directPB.mkdir();
                if (create) {
                    //System.out.println("Database directory created successfully!");
                } else {
                    System.err.println("Failed to create database directory!");
                    System.err.println("ERROR");
                    return;
                }
            } else {
                //System.out.println("Database directory already exists!");
            }

            String pathPage = db_loc + "/Tables";
            File pathDir = new File(pathPage);
            if (!pathDir.exists()) {
                pathDir.mkdir();
            }
            File directPage = new File(pathPage);
            if (!directPage.exists()) {
                boolean created = directPage.mkdir();
                if (created) {
                    //System.out.println("Main.Page directory created successfully!");
                } else {
                    System.err.println("Failed to create page directory!");
                    System.err.println("ERROR");
                    return;
                }
            } else {
               // System.out.println("Main.Page directory already exists!");
            }

        } else {
            System.out.println("Looking at " + db_loc + " for existing database...");
            System.out.println("Database found...");
            System.out.println("Restarting the database...");
        }

        System.out.println("Main.Page Size: " + page_size);
        System.out.println("Buffer Size: " + buffer_size);

        //Main.StorageManager storageManager = new Main.StorageManager(db_loc, page_size, buffer_size);
        PageBuffer pageBuffer = new PageBuffer(db_loc,buffer_size,page_size);


        System.out.println("----------------------------------------------");
        System.out.println("Welcome to Storage Manager!");
        //System.out.println("Database's now running...");
        displayCommand();

        while(true) {
            System.out.println("\nPlease enter commands, enter <quit> to shutdown the database > ");
            String input = scanner.nextLine().trim();
            String[] optionArr = input.split(" ");
            if (input.equals("display schema;")) {
                Catalog catalog = pageBuffer.getCatalog();
                pageBuffer.getCatalog().displaySchema(args[0], page_size, buffer_size, catalog);

            } else if (optionArr[0].equals("display") && optionArr[1].equals("info") && optionArr.length == 3) {
                if (optionArr[2].charAt(optionArr[2].length()-1) == ';') {
                    optionArr[2] = optionArr[2].substring(0, optionArr[2].length() - 1);
                }
                Table table = pageBuffer.getStorageManager().getTableByName(optionArr[2]);
                if (table != null) {
                    pageBuffer.getCatalog().displayInfoTable(table);
                    System.out.println("\nSUCCESS");
                }

            } else if (optionArr[0].equals("create") && optionArr[1].equals("table") && optionArr.length > 3) {
                if (pageBuffer.getStorageManager().createTable(input) != null) {
                    System.out.println("\nSUCCESS");
                }

            } else if (input.equals("<quit>")) {
                if (pageBuffer.quitProgram(pageBuffer.getStorageManager(), pageBuffer.getPagelistBuffer())) {
                    System.out.println("Safely shutting down the database...");
                    System.out.println("Writing pages in the page buffer successfully...");
                    System.out.println("Saving catalog successfully...");;
                    System.out.println("\nExisting the database...");
                } else {
                    System.err.println("\nERROR");
                }
                break;

            } else if (optionArr[0].equals("insert") && optionArr[1].equals("into") && optionArr[3].equals("values") && optionArr.length >= 5) {
                pageBuffer.insertRecordToTable(input, optionArr[2]);

            } else if (optionArr[0].equals("select") && optionArr[1].equals("*") && optionArr[2].equals("from") && optionArr.length == 4) {
                if (optionArr[3].charAt(optionArr[3].length()-1) == ';') {
                    optionArr[3] = optionArr[3].substring(0, optionArr[3].length() - 1);
                }
                Table table = pageBuffer.getStorageManager().getTableByName(optionArr[3]);
                if (table != null) {
                    pageBuffer.selectStarFromTable(table);
                    System.out.println("\nSUCCESS");
                }


            } else if (optionArr[0].equals("get") && optionArr[1].equals("page") && optionArr[3].equals("in") &&
                    optionArr[4].equals("table") && optionArr.length==6) {
                if (optionArr[5].charAt(optionArr[5].length()-1) == ';') {
                    optionArr[5] = optionArr[5].substring(0, optionArr[5].length() - 1);
                }
                Table table = pageBuffer.getStorageManager().getTableByName(optionArr[5]);
                if (isInteger(optionArr[2])) {
                    if (table != null) {
                        if (pageBuffer.getPageByTableAndPageNumber(table, Integer.parseInt(optionArr[2]))) {
                            System.out.println("\nSUCCESS");
                        }
                    } else {
                        System.err.println("No page in the table.");
                        System.err.println("ERROR");
                    }
                } else {
                    System.err.println("pageNum has to be a number!");
                    System.err.println("ERROR");
                }

            } else if (optionArr[0].equals("select") && optionArr[1].equals("record") && optionArr[2].equals("from") &&
                    optionArr[4].equals("where") && optionArr[5].equals("primarykey") && optionArr.length == 7) {
                if (optionArr[6].charAt(optionArr[6].length() - 1) == ';') {
                    optionArr[6] = optionArr[6].substring(0, optionArr[6].length() - 1);
                }
                Table table = pageBuffer.getStorageManager().getTableByName(optionArr[3]);
                if (table != null) {
                    Record record = pageBuffer.getRecordByPrimaryKey(optionArr[6], table);
                    if (record != null) {
                        System.out.println(pageBuffer.getRecordByPrimary(record));
                        System.out.println("\nSUCCESS");
                    } else {
                        System.err.println("No record with that primary key");
                        System.err.println("ERROR");
                    }
                } else {
                    System.err.println("No such table " + optionArr[6]);
                    System.err.println("ERROR");
                }

            } else if (optionArr[0].equals("drop") && optionArr[1].equals("table") && optionArr.length == 3) {
                if (optionArr[2].charAt(optionArr[2].length() - 1) == ';') {
                    optionArr[2] = optionArr[2].substring(0, optionArr[2].length() - 1);
                }
                if (pageBuffer.getStorageManager().dropTable(optionArr[2])) {
                    System.out.println("\nSUCCESS");
                } else {
                    System.err.println("No such table " + optionArr[2]);
                    System.err.println("ERROR");
                }

            } else {
                System.err.println("It is not a valid command.");

            }
        }
    }

    /**
     * Method displays the command list
     */
    public static void displayCommand() {
        System.out.println("List of commands:");
        System.out.println("    display schema;");
        System.out.println("    display info <name>;");
        System.out.println("    select * from <name>;");
        System.out.println("    insert into <name> values <tuples>;");
        System.out.println("    create table <name> (");
        System.out.println("        <attr_name1> <attr_type1> primarykey,");
        System.out.println("        <attr_name2> <attr_type2>,...");
        System.out.println("        <attr_nameN> <attr_typeN> );");
        System.out.println("    get page <pageNum> in table <name>;" );
        System.out.println("    select record from <tableName> where primarykey <primarykey_value>;");
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
