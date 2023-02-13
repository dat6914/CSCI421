import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * The goal of this class is to communicate with hardware
 */
public class StorageManager {
    private String db_loc;
    private int page_size;
    private int buffer_size;
    private Catalog catalog;

    public StorageManager(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
        this.catalog = new Catalog(db_loc);
    }

    //save everything
    //restore database



}
