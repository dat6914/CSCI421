

public class Database {

    public static Database database  = null;
    public static StorageManager storageManager;
    public static Catalog catalog;

    public Database(String db_loc, int page_size, int buffer_size) {
        storageManager = new StorageManager(db_loc, page_size, buffer_size);
        //create datalog

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
}
