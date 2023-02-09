import java.util.ArrayList;

public class Record {
      private ArrayList<Object> values_list = new ArrayList<>();
      private String primaryKey;

      public Object getObject(int i) {
            return values_list.get(i);
      }

      // write to page tao lam
      /**
       * Row2: 1 | taolao | sha | 3.3 | false
       * 0010101020101/01010101010
       * 0,4: 4 dang byte array
       *
       * 0,4: 1 dang byte array
       * 4, 8: 6 (size of taolao) int 4 byte
       * 8, 14: taolao bytearray
       * 14,  29; sha0 /0
       * 29, 37: 3.3 dang byte array
       * 37, 38: 0
       */
      public static byte[] convertRecordTextToBinary() {

            return null;
      }


}
