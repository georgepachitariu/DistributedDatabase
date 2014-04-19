package TestingUnit.Data;

import ConsistentHashing.HelpingClasses.HashingFunction;
import Data.LocalDataSystem;
import Data.ImageWithMetadata;
import Data.LocalImageStorage;
import Data.LocalTagStorage;
import junit.framework.Assert;
import org.junit.Test;

import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/23/13
 * Time: 2:14 PM
 */
public class LocalDataSystemTest {

    @Test
    public void testInsertImage_goodScenario() {

        ImageWithMetadata img= new ImageWithMetadata();

        HashingFunction mockHashFct=mock(HashingFunction.class);
        when(mockHashFct.getImageHash(img)).thenReturn((long)100);

        LocalImageStorage st=mock(LocalImageStorage.class);
        when(st.containsInRange((long)100)).thenReturn(true);
        when(st.insert("100",img)).thenReturn(true);

        LinkedList<LocalImageStorage> list = new LinkedList<LocalImageStorage>();
        list.add(st);

        LocalDataSystem test = new LocalDataSystem(mockHashFct,list,null,"");
        boolean result = test.insertImage(img);

        Assert.assertEquals(true,result );
     }

    @org.junit.Test
    public void testInsertImage_goodBad() {

        ImageWithMetadata img= new ImageWithMetadata();

        HashingFunction mockHashFct=mock(HashingFunction.class);
        when(mockHashFct.getImageHash(img)).thenReturn((long) 100);

        LocalImageStorage st=mock(LocalImageStorage.class);
        when(st.containsInRange((long)100)).thenReturn(true);
        when(st.insert("100", img)).thenReturn(false);

        LocalDataSystem test = new LocalDataSystem(  mockHashFct,
                    new LinkedList<LocalImageStorage>(),  null,"" );
        boolean result = test.insertImage(img);

        Assert.assertEquals(false,result );
    }

    @org.junit.Test
    public void testInsertTag_goodScenario() {

        String tag="fox";

        HashingFunction mockHashFct=mock(HashingFunction.class);
        when(mockHashFct.getTagHash(tag)).thenReturn((long)100);

        LocalTagStorage st=mock(LocalTagStorage.class);
        when(st.containsInRange((long)100)).thenReturn(true);
        when(st.insert(100,(long)150)).thenReturn(true);

        LinkedList<LocalTagStorage> list = new LinkedList<LocalTagStorage>();
        list.add(st);

        LocalDataSystem test = new LocalDataSystem(mockHashFct,null,list,"");
        boolean result = test.insertTag("fox", 150);

        Assert.assertEquals(true,result );
    }

    @org.junit.Test
    public void testInsertTag_badScenario() {
        String tag="fox";

        HashingFunction mockHashFct=mock(HashingFunction.class);
        when(mockHashFct.getTagHash(tag)).thenReturn((long)100);

        LocalTagStorage st=mock(LocalTagStorage.class);
        when(st.containsInRange((long)100)).thenReturn(true);
        when(st.insert(100,(long)150)).thenReturn(false);

        LocalDataSystem test = new LocalDataSystem(mockHashFct,null,
                            new LinkedList<LocalTagStorage>(),"" );
        boolean result = test.insertTag("fox", 150);

        Assert.assertEquals(false,result );
    }

    @org.junit.Test
    public void testGetImage_goodScenario() {

        ImageWithMetadata img=new ImageWithMetadata();

        LocalImageStorage stMock=mock(LocalImageStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.getImage("100")).thenReturn(img);

        LinkedList<LocalImageStorage> list =new LinkedList<LocalImageStorage>();
        list.add(stMock);

        LocalDataSystem dS=new LocalDataSystem(null,list,null,"");
        ImageWithMetadata result = dS.getImage((long) 100);

        Assert.assertEquals(img, result);
    }

    @org.junit.Test
    public void testGetImage_badScenario() {
        LocalImageStorage stMock=mock(LocalImageStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.getImage("100")).thenReturn(null);

        LinkedList<LocalImageStorage> list =new LinkedList<LocalImageStorage>();

        LocalDataSystem dS=new LocalDataSystem(null,list,null,"");
        ImageWithMetadata result = dS.getImage((long) 100);

        Assert.assertEquals(null, result);
    }

    @org.junit.Test
    public void testGetImageHashes_goodScenario() {

        LinkedList<Long> coll = new LinkedList<Long>();

        HashingFunction mockHash=mock(HashingFunction.class);
        when(mockHash.getTagHash("fox")).thenReturn((long)100);

        LocalTagStorage str= mock(LocalTagStorage.class);
        when(str.containsInRange((long)100)).thenReturn(true);
        when(str.getImageValueHashCollection((long)100)).thenReturn(coll);

        LinkedList<LocalTagStorage> mockImageStorage =
                new LinkedList<LocalTagStorage>();
        mockImageStorage.add(str);

       LocalDataSystem test=new LocalDataSystem(mockHash,null, mockImageStorage,"");
        LinkedList<Long> result =test.getImageHashes("fox");
        Assert.assertEquals(coll, result);
    }

    @org.junit.Test
    public void testGetImageHashes_badScenario() {

        HashingFunction mockHash=mock(HashingFunction.class);
        when(mockHash.getTagHash("fox")).thenReturn((long)100);

        LocalTagStorage str= mock(LocalTagStorage.class);
        when(str.containsInRange((long)100)).thenReturn(true);
        when(str.getImageValueHashCollection((long)100)).thenReturn(null);

        LinkedList<LocalTagStorage> mockImageStorage =
                new LinkedList<LocalTagStorage>();
        mockImageStorage.add(str);

        LocalDataSystem test=new LocalDataSystem(mockHash,null, mockImageStorage,"");
        LinkedList<Long> result =test.getImageHashes("fox");
        Assert.assertEquals(null, result);
    }

    @org.junit.Test
    public void testDeleteImage_goodScenario() {

        LinkedList<String> returnList = new LinkedList<String>();

        LocalImageStorage stMock=mock(LocalImageStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.deleteImage("100")).thenReturn(returnList);

        LinkedList<LocalImageStorage> list =new LinkedList<LocalImageStorage>();
        list.add(stMock);

        LocalDataSystem dS=new LocalDataSystem(null,list,null,"");
        LinkedList<String> result = dS.deleteImage((long) 100);

        Assert.assertEquals(returnList, result);
    }

    @org.junit.Test
    public void testDeleteImage_badScenario() {

        LocalImageStorage stMock=mock(LocalImageStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.deleteImage("100")).thenReturn(null);

        LinkedList<LocalImageStorage> list =new LinkedList<LocalImageStorage>();
        list.add(stMock);

        LocalDataSystem dS=new LocalDataSystem(null,list,null,"");
        LinkedList<String> result = dS.deleteImage((long) 100);

        Assert.assertEquals(null, result);
    }

    @org.junit.Test
    public void testDeleteTag_goodScenario() {

        LocalTagStorage stMock=mock(LocalTagStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.delete((long)100, 150)).thenReturn(true);

        LinkedList<LocalTagStorage> list =new LinkedList<LocalTagStorage>();
        list.add(stMock);

        LocalDataSystem dS=new LocalDataSystem(null,null,list,"");
        boolean result = dS.deleteTag((long) 100, (long) 150);

        Assert.assertEquals(true, result);
    }

    @org.junit.Test
    public void testDeleteTag_badScenario() {

        LocalTagStorage stMock=mock(LocalTagStorage.class);
        when(stMock.containsInRange(100)).thenReturn(true);
        when(stMock.delete((long)100, 150)).thenReturn(false);

        LinkedList<LocalTagStorage> list =new LinkedList<LocalTagStorage>();
        list.add(stMock);

        LocalDataSystem dS=new LocalDataSystem(null,null,list,"");
        boolean result = dS.deleteTag((long) 100, (long) 150);

        Assert.assertEquals(false, result);
    }
}
