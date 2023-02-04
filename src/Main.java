public class Main {
    public static void main(String[] args) {
        System.out.println("Storage Manager!");
        if (args.length != 3) {
            System.err.println("Usage: <db_loc> <page_size> <buffer_size>");
        }
        String db_loc = args[0];
        int page_size = Integer.parseInt(args[1]);
        int buffer_size = Integer.parseInt(args[2]);
        StorageManager storageManager = new StorageManager(db_loc, page_size, buffer_size, false);



    }
}
