package Test;


import Main.Page;
import Main.Pointer;
import Main.Record;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import Main.PageBuffer;

import java.util.ArrayList;

public class TableTest {
    @Test
    public void test(){
        ArrayList<Pointer> pointerArrayList = new ArrayList<Pointer>();
        ArrayList<Record> records = new ArrayList<Record>();


        ArrayList<Object> valueList = new ArrayList<>();
        valueList.add(3);
        valueList.add(3.7);
        valueList.add("hi");
        ArrayList<String> attributeTypeList = new ArrayList<>();
        attributeTypeList.add("3");
        attributeTypeList.add("4");
        attributeTypeList.add("52");
        Record record = new Record(valueList,attributeTypeList);
        Record record1 = new Record(valueList,attributeTypeList);
        int len1 = record.computeRecordSize();
        int len2 = record1.computeRecordSize();
        Pointer pointer = new Pointer(4059,len1);
        Pointer pointer1 = new Pointer(4059-len2,len2);
        int numRec = 2;
        int pageId = 1;
        int pagesize = 4096;
        records.add(record);
        records.add(record1);
        pointerArrayList.add(pointer);
        pointerArrayList.add(pointer1);

        Page page1 = new Page(numRec,pageId,pointerArrayList,records);
        byte[] pageByte = page1.convertPageToByteArr(page1,pagesize);
        Page page2  = Page.convertByteArrToPage(pageByte);
        String path = "TableSerializeTest.txt";
        PageBuffer.writeToDiskWithRandomAccess(path, page1, 4, pagesize, 1);
        Page mypage = PageBuffer.readFromDiskWithRandomAccess(path, 4, pagesize);
        assertEquals(page2, mypage);

    }

}
