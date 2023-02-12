package DBImplementation;

import java.util.ArrayList;

public class Record {
      private ArrayList<Object> values_list = new ArrayList<>();
      private String primaryKey;
      private int size;

      public Record(String primaryKey,ArrayList<Object> values_list, int size){
            this.primaryKey = primaryKey;
            this.values_list = values_list;
            this.size = size;
      }

      public Object getObject(int i) {
            return values_list.get(i);
      }

      public static byte[] convertRecordTextToBinary() {
            return null;
      }

      public int getSize(){return this.size;}

      public String getPrimaryKey(){return this.primaryKey;}
}
