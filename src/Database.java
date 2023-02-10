import java.lang.reflect.Array;
import java.util.ArrayList;

public class Database {

    public static Database database  = null;
    public static StorageManager storageManager;
    public static Catalog catalog;
    private final String db_loc;
    private final int page_size;
    private final int buffer_size;

    public Database(String db_loc, int page_size, int buffer_size) {
        this.storageManager = new StorageManager(db_loc, page_size, buffer_size);
        this.catalog = new Catalog();
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;

    }

    public static Database getConnection(String db_loc, int page_size, int buffer_size) {
        if (database != null) {
            return database;
        } else {
            database = new Database(db_loc, page_size, buffer_size);
            return database;
        }
    }

    //terminate the database
    public void terminateDatabase() {

    }

    /**
     * This function displays the schema of the db (the catalog itself) in the following format:
     * DB location: <db_loc>
     * Page size: <page size>
     * Buffer size: <buffer size>
     * Tables:
     * (Same as displayTableInfo but for all tables in the catalog
     */
    public void displaySchema(){
        System.out.println("DB location: " + this.db_loc);
        System.out.println("Page size: " + this.page_size);
        System.out.println("Buffer size: " + this.buffer_size);
        System.out.println("Tables: \n");

        for (Table table : catalog.tables_list) {
            catalog.displayTableInfo(table.getTableName());
        }

        System.out.println("\nSUCCESS\n");
    }
}
