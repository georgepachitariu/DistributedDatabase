package TestingFunctionality;

import ConsistentHashing.HashRange;
import ConsistentHashing.HashingFunction;
import Data.LocalDataSystem;
import Data.ImageWithMetadata;
import Data.LocalImageStorage;
import Data.LocalTagStorage;
import junit.framework.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/1/13
 * Time: 1:20 PM
 */
public class TestingDataSystem_Local {
    @org.junit.Test
    public void DataTest() throws IOException {
        // Functionality test
        // Steps:
        // 1. Create a LocalDataSystem
        // 2. We add 3 virtual Data Nodes
        // 3. We add and
        // 4. We search for images and assert that they are found
        // 5. We delete the images;
        // 6. We search for images and assert that they are NOT found

        // 7. We add 3 virtual Tag Nodes
        // 8. We add tags
        // 9. We search for tags and assert that they are found
        // 10. We delete the tags;
        // 11. We search for tags and assert that they are NOT found


        String root="test/DemoStorage/";
        this.deleteFiles(root);

        // 1. Create a LocalDataSystem
        HashingFunction hashFunction = mock( HashingFunction.class);
        when(hashFunction.getImageHash(any(ImageWithMetadata.class))).
                thenReturn((long)100).thenReturn((long) 200).thenReturn((long) 300);
        LocalDataSystem LocalDataSystem = new LocalDataSystem(hashFunction ,new LinkedList<LocalImageStorage>(),
                new LinkedList<LocalTagStorage>(), root );

        // 2. We add 3 virtual Data Nodes
        //these are hash Ranges for which this system is responsible.
        HashRange range1=new HashRange(0, 250);
        HashRange range2 = new HashRange(270, 500);
        LocalDataSystem.addDataVirtualNode(range1);
        LocalDataSystem.addDataVirtualNode(range2);

        // 3. Insert images
        boolean succeeded;
        String imagesFolder="test/ImagesData/";
        ImageWithMetadata str1= new ImageWithMetadata();
        str1.fileName="the_tiger_and_the_snow.jpg";
        String path=imagesFolder+str1.fileName;
        str1.raw= Files.readAllBytes(Paths.get(path));
        str1.tags.add("tiger");
        str1.tags.add("snow");
        succeeded=LocalDataSystem.insertImage(str1);
        Assert.assertEquals(true, succeeded );

        ImageWithMetadata str2= new ImageWithMetadata();
        str2.fileName="snow-free-tiger-in-the-and-backgrounds-187625.jpg";
        path=imagesFolder+str2.fileName;
        str2.raw= Files.readAllBytes(Paths.get(path));
        str2.tags.add("tiger");
        str2.tags.add("snow");
        succeeded=LocalDataSystem.insertImage(str2);
        Assert.assertEquals(true, succeeded );

        ImageWithMetadata str3= new ImageWithMetadata();
        str3.fileName="Red Fox Wallpapers 8.jpg";
        path=imagesFolder+str3.fileName;
        str3.raw= Files.readAllBytes(Paths.get(path));
        str3.tags.add("fox");
        str3.tags.add("snow");
        succeeded=LocalDataSystem.insertImage(str3);
        Assert.assertEquals(true, succeeded);


        //4. we search for images
        ImageWithMetadata var = LocalDataSystem.getImage(100);
        org.junit.Assert.assertArrayEquals(str1.toBytes(), LocalDataSystem.getImage(100).toBytes() );
        org.junit.Assert.assertArrayEquals( str2.toBytes(), LocalDataSystem.getImage(200).toBytes() );
        org.junit.Assert.assertArrayEquals( str3.toBytes(), LocalDataSystem.getImage(300).toBytes() );


        // 5. We delete the images;
        LinkedList<String > list=LocalDataSystem.deleteImage(100);
        Assert.assertEquals (true, list.contains("tiger")&&list.contains("snow")&& list.size()==2 );
        Assert.assertNull(LocalDataSystem.deleteImage(100));
        list=LocalDataSystem.deleteImage(200);
        Assert.assertEquals (true,  list.contains("tiger")&&list.contains("snow")&& list.size()==2);
        list=LocalDataSystem.deleteImage(300);
        Assert.assertEquals (true, list.contains("fox")&&list.contains("snow")&& list.size()==2);

        // 6. We search for images and assert that they are NOT found
        Assert.assertNull(LocalDataSystem.getImage(100));
        Assert.assertNull(LocalDataSystem.getImage(200));
        Assert.assertNull(LocalDataSystem.getImage(300));

        this.deleteFiles(root);
    }


    public void deleteFiles(String folderName) {
        for(File f : new File(folderName).listFiles())
            f.delete();
    }
}
