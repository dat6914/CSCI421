import java.util.ArrayList;

public class Record {
      private ArrayList<Object> values_list = new ArrayList<>();
      private String primaryKey;

      public Object getObject(int i) {
            return values_list.get(i);
      }


      public static byte[] convertRecordTextToBinary() {
            return null;
      }

      public static void start(){
            System.out.println("Hello");
      }
}
