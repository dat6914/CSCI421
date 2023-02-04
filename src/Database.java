public class Database {
    String db_loc;
    int page_size;
    int buffer_page;

    public Database(String db_loc, int page_size, int buffer_page) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_page = buffer_page;
    }
}
