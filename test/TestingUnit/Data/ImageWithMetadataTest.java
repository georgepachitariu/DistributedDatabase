package TestingUnit.Data;

import Data.ImageWithMetadata;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/21/13
 * Time: 9:54 PM
 */
public class ImageWithMetadataTest {

    @Test    //the testing worked on formats: jpg, png, gif, bmp
    public void  TestingWritingAndReadingStructureToDisk_Usualscenario() {

        // Usual scenario
        ////////........................
        //Creating structure 'a'
        ImageWithMetadata a= new ImageWithMetadata();
        a.tags.add("fox");
        a.tags.add("2013");
        a.tags.add("snow");
        a.fileName="Fox in the snow";
        String path="test/DataForTesting/fox-snow.jpg";
        try {
            a.raw= Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Writing it to disk
        String wr_path="test/DataForTesting/fox_snow_2013.txt";
        a.writeToDisk(wr_path);

        // Reading (&creating) from disk structure 'b'
        ImageWithMetadata b= new ImageWithMetadata(Paths.get(wr_path));

        // Verifying that structure 'a' equals to 'b'
        for(String tag : a.tags)
            Assert.assertEquals(b.tags.contains(tag), true);
        Assert.assertEquals(a.fileName, b.fileName);
        org.junit.Assert.assertArrayEquals(a.raw,b.raw);

        new File(wr_path).delete();
    }

    @Test
    public void  TestingWritingAndReadingStructureToDisk_EmptyNameAndTags() {

        //Creating structure 'a'
        ImageWithMetadata a= new ImageWithMetadata();
        String path="test/DataForTesting/fox-snow.jpg";
        try {
            a.raw= Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Writing it to disk
        String wr_path="test/DataForTesting/fox_snow_2013.txt";
        a.writeToDisk(wr_path);

        // Reading (&creating) from disk structure 'b'
        ImageWithMetadata b= new ImageWithMetadata(Paths.get(wr_path));

        // Verifying that structure 'a' equals to 'b'
        org.junit.Assert.assertArrayEquals(a.raw,b.raw);

        new File(wr_path).delete();
    }

    @Test
    public void  TestingToBytes_FromBytes() {
        // Usual scenario
        ////////........................
        //Creating structure 'a'
        ImageWithMetadata a= new ImageWithMetadata();
        a.tags.add("fox");
        a.tags.add("2013");
        a.tags.add("snow");
        a.fileName="Fox in the snow";
        String path="test/DataForTesting/fox-snow.jpg";
        try {
            a.raw= Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Reading (&creating) from bytes structure 'b'
        ImageWithMetadata b= new ImageWithMetadata( a.toBytes());

        // Verifying that structure 'a' equals to 'b'
        for(String tag : a.tags)
            Assert.assertEquals(b.tags.contains(tag), true);
        Assert.assertEquals(a.fileName, b.fileName);
        org.junit.Assert.assertArrayEquals(a.raw,b.raw);
    }
}
