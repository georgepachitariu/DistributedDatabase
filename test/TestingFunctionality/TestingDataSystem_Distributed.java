package TestingFunctionality;

import ConsistentHashing.DistributionManager;
import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.HashingFunction;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import Data.LocalDataSystem;
import Interface.ClientTools;
import Interface.DatabaseConnection;
import junit.framework.Assert;
import NetworkInfrastructure.IncomingConnectionsThread;
import NetworkInfrastructure.NetworkCommands.CCloseThread;
import NetworkInfrastructure.ServerNetworkInfo;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/5/13
 * Time: 10:55 AM
 */
public class TestingDataSystem_Distributed {

    @org.junit.Test
    public void DataTest() throws IOException, InterruptedException {

     // 1. We create the images to be inserted
        String imagesFolder="test/ImagesData/";
        ImageWithMetadata str1= new ImageWithMetadata();
        str1.fileName="fox snow web.jpg";
        String path=imagesFolder+str1.fileName;
        str1.raw= Files.readAllBytes(Paths.get(path));
        str1.tags.add("fox");
        str1.tags.add("snow");

        ImageWithMetadata str2= new ImageWithMetadata();
        str2.fileName="Siberian_Tiger_2.jpg";
        path=imagesFolder+str2.fileName;
        str2.raw= Files.readAllBytes(Paths.get(path));
        str2.tags.add("tiger");
        str2.tags.add("snow");

        ImageWithMetadata str3= new ImageWithMetadata();
        str3.fileName="snow-free-tiger-in-the-and-backgrounds-187625.jpg";
        path=imagesFolder+str3.fileName;
        str3.raw= Files.readAllBytes(Paths.get(path));
        str3.tags.add("tiger");
        str3.tags.add("snow");


    // 1. We create two DatabaseSystem's
        String root="test/DemoStorage/";

        HashingFunction mockHashingFunction=mock(HashingFunction.class);
        when(mockHashingFunction.getImageHash(str1)).thenReturn((long)37);
        when(mockHashingFunction.getImageHash(str2)).thenReturn((long)42);
        when(mockHashingFunction.getImageHash(str3)).thenReturn((long)189);
        when(mockHashingFunction.getTagHash("fox")).thenReturn((long) 142);
        when(mockHashingFunction.getTagHash("snow")).thenReturn((long) 56);
        when(mockHashingFunction.getTagHash("tiger")).thenReturn((long) 26);

        LinkedList<ServerNetworkInfo> allServers=new LinkedList<ServerNetworkInfo>();
        allServers.add(new ServerNetworkInfo("127.0.0.1:7001"));
        allServers.add(new ServerNetworkInfo("127.0.0.1:7002"));

        // Server1
        LocalDataSystem localSystem= new LocalDataSystem(mockHashingFunction , root+"1/");
        localSystem.addDataVirtualNode(new HashRange(0, 100));
        localSystem.addTagVirtualNode(new HashRange(0, 100));

        DatabaseSystem databaseSystem1=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:7001"),
                localSystem,null);
        databaseSystem1.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem1));

        // Server2
        LocalDataSystem localSystem2= new LocalDataSystem(mockHashingFunction , root+"2/");
        localSystem2.addDataVirtualNode(new HashRange(100,200));
        localSystem2.addTagVirtualNode(new HashRange(100,200));

        DatabaseSystem databaseSystem2=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:7002"),
                localSystem2, null);
        databaseSystem2.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem2));

        LinkedList <ServerSegmentsStruct> list=new LinkedList<ServerSegmentsStruct>();
        list.add(databaseSystem1.getServerRanges());
        list.add(databaseSystem2.getServerRanges());

        DistributionManager distr1=new DistributionManager(1,1);
        distr1.setDatabaseSystem(databaseSystem1);
        distr1.setExistingServers(list);
        databaseSystem1.setDistributionManager(distr1);

        DistributionManager distr2=new DistributionManager(1,1);
        distr1.setDatabaseSystem(databaseSystem2);
        distr2.setExistingServers(list);
        databaseSystem2.setDistributionManager(distr2);

        ///////////////////////////////////////////////////////////////////////////

        // 2. We create a connection from the client
        DatabaseConnection connection=new DatabaseConnection(
                allServers, mockHashingFunction, new ClientTools(), new Random());

        // 3. We insert the image
        connection.insert(str1);
        connection.insert(str2);
        connection.insert(str3);

        // 4. We make the assertions
        // For image1:
        org.junit.Assert.assertArrayEquals
                (str1.toBytes(), localSystem.getImage((long) 37).toBytes());
        Assert.assertEquals(true, localSystem.getImageHashes("snow").contains((long)37));
        Assert.assertEquals(true, localSystem2.getImageHashes("fox").contains((long)37));


        // For image2:
        org.junit.Assert.assertArrayEquals
                (str2.toBytes(), localSystem.getImage((long) 42).toBytes());
        Assert.assertEquals(true, localSystem.getImageHashes("snow").contains((long)42));
        Assert.assertEquals(true, localSystem.getImageHashes("tiger").contains((long)42));

        // For image3:
        org.junit.Assert.assertArrayEquals
                (str3.toBytes(), localSystem2.getImage((long) 189).toBytes());
        Assert.assertEquals(true, localSystem.getImageHashes("snow").contains((long)189));
        Assert.assertEquals(true, localSystem.getImageHashes("tiger").contains((long)189));


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // We search for images in the database
        LinkedList<String> tags = new LinkedList<String>();
        tags.add("tiger");
        LinkedList<ImageWithMetadata> returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));

        tags = new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str1));
        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // We delete an image str1 from the database

        boolean result=connection.delete(str1);
        Assert.assertTrue(result);

        // We search again for image "str1" in the database
        tags = new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);
        // And we assert that we don't find it now
        Assert.assertEquals(false, returnedList.contains(str1));


        // We tell the database servers to close their listening threads
        CCloseThread command= new CCloseThread(databaseSystem1.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(databaseSystem2.getServerNetworkInfo());
        command.request();
    }

    @After
    public void tearDown() throws IOException {

        // delete sub-Folders
        String folderName="test/DemoStorage/";
        File[] databaseFolders=new File(folderName).listFiles();
        for(File f :databaseFolders)
            FileUtils.deleteDirectory(f);
    }

    @Before
    public void cleaningMethod() throws IOException {

        // delete sub-Folders
        String folderName="test/DemoStorage/";
        File[] databaseFolders=new File(folderName).listFiles();
        for(File f :databaseFolders)
            FileUtils.deleteDirectory(f);
    }
}
