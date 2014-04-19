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
import org.junit.Test;

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
 * Date: 5/17/13
 * Time: 5:17 PM
 */
public class TestingDistributionManager {

    @Test
    public void TestGetNewVirtualDataNode_andDataMoving() throws IOException {

        // Functionality test
        // Steps:
        // 1. Create 2 DataBaseSystem: A and B
        // 2. Insert 2 images in B
        // 3. Get new virtual data node in A (which contains the 2 images)
        // 4. Assert that the images were copied in A

        // . We create the images to be inserted
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


        // 1.Create 2 DataBaseSystem: A and B
        String root="test/DemoStorage/";

        HashingFunction mockHashingFunction=mock(HashingFunction.class);
        when(mockHashingFunction.getImageHash(str1)).thenReturn((long)167);
        when(mockHashingFunction.getImageHash(str2)).thenReturn((long)172);
        when(mockHashingFunction.getTagHash("fox")).thenReturn((long) 142);
        when(mockHashingFunction.getTagHash("snow")).thenReturn((long) 56);
        when(mockHashingFunction.getTagHash("tiger")).thenReturn((long) 26);

        LinkedList<ServerNetworkInfo> allServers=new LinkedList<ServerNetworkInfo>();
        allServers.add(new ServerNetworkInfo("127.0.0.1:9001"));
        allServers.add(new ServerNetworkInfo("127.0.0.1:9002"));

        // Server A
        LocalDataSystem localSystem= new LocalDataSystem(mockHashingFunction , root+"1/");
        localSystem.addDataVirtualNode(new HashRange(0, 100));
        localSystem.addTagVirtualNode(new HashRange(0, 100));

        DatabaseSystem databaseSystem_A=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:9001"),
                localSystem,null);
        databaseSystem_A.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem_A));

        // Server B
        LocalDataSystem localSystem2= new LocalDataSystem(mockHashingFunction , root+"2/");
        localSystem2.addDataVirtualNode(new HashRange(100,200));
        localSystem2.addTagVirtualNode(new HashRange(100,200));

        DatabaseSystem databaseSystem_B=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:9002"),
                localSystem2, null);
        databaseSystem_B.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem_B));

        LinkedList <ServerSegmentsStruct> list=new LinkedList<ServerSegmentsStruct>();
        list.add(databaseSystem_A.getServerRanges());
        list.add(databaseSystem_B.getServerRanges());

        Random rand=mock(Random.class);
        when(rand.nextLong()).thenReturn((long)150);

        DistributionManager distr1=new DistributionManager( rand, 1,1);
        distr1.setDatabaseSystem(databaseSystem_A);
        distr1.setExistingServers(list);
        databaseSystem_A.setDistributionManager(distr1);

        DistributionManager distr2=new DistributionManager(1,1);
        distr2.setDatabaseSystem(databaseSystem_B);
        distr2.setExistingServers(list);
        databaseSystem_B.setDistributionManager(distr2);

        // 2. Insert 2 images in B
             // 2.1 We create a connection from the client
        DatabaseConnection connection=new DatabaseConnection(
                allServers, mockHashingFunction, new ClientTools(), new Random());

            // 2.3 We insert the images
        connection.insert(str1);
        connection.insert(str2);

        // 3. Get new virtual data node in A (that contains the 2 images)
        // (The new virtual node has range 150-200)
        databaseSystem_A.getDistributionManager().attachNewDataVirtualNode();

        // 4. Assert that the images were copied in A
        Assert.assertEquals(str1, databaseSystem_A.getLocalDataSystem().getImage(167));
        Assert.assertEquals(str2, databaseSystem_A.getLocalDataSystem().getImage(172));

        // We tell the database servers to close their listening threads
        CCloseThread command= new CCloseThread(databaseSystem_A.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(databaseSystem_B.getServerNetworkInfo());
        command.request();
    }

    @Test
    public void TestGetNewVirtualTagNode_andTagsMoving() throws IOException {

        // Functionality test
        // Steps:
        // 1. Create 2 DataBaseSystem: A and B
        // 2. Insert 2 images with their tags in B
        // 3. Get new virtual data node in A (which contains the tags)
        // 4. Assert that the tags were copied in A

        // . We create the images to be inserted
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


        // 1.Create 2 DataBaseSystem: A and B
        String root="test/DemoStorage/";

        HashingFunction mockHashingFunction=mock(HashingFunction.class);
        when(mockHashingFunction.getImageHash(str1)).thenReturn((long)167);
        when(mockHashingFunction.getImageHash(str2)).thenReturn((long)172);
        when(mockHashingFunction.getTagHash("fox")).thenReturn((long) 162);
        when(mockHashingFunction.getTagHash("snow")).thenReturn((long) 156);
        when(mockHashingFunction.getTagHash("tiger")).thenReturn((long) 176);

        LinkedList<ServerNetworkInfo> allServers=new LinkedList<ServerNetworkInfo>();
        allServers.add(new ServerNetworkInfo("127.0.0.1:9003"));
        allServers.add(new ServerNetworkInfo("127.0.0.1:9004"));

        // Server A
        LocalDataSystem localSystem= new LocalDataSystem(mockHashingFunction, root+"1/");
        localSystem.addDataVirtualNode(new HashRange(0, 100));
        localSystem.addTagVirtualNode(new HashRange(0, 100));

        DatabaseSystem databaseSystem_A=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:9003"),
                localSystem,null);
        databaseSystem_A.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem_A));

        // Server B
        LocalDataSystem localSystem2= new LocalDataSystem(mockHashingFunction , root+"2/");
        localSystem2.addDataVirtualNode(new HashRange(100,200));
        localSystem2.addTagVirtualNode(new HashRange(100,200));

        DatabaseSystem databaseSystem_B=new DatabaseSystem(
                new ServerNetworkInfo("127.0.0.1:9004"),
                localSystem2, null);
        databaseSystem_B.setIncomingConnectionsThread(
                new IncomingConnectionsThread(databaseSystem_B));

        LinkedList <ServerSegmentsStruct> list=new LinkedList<ServerSegmentsStruct>();
        list.add(databaseSystem_A.getServerRanges());
        list.add(databaseSystem_B.getServerRanges());

        Random rand=mock(Random.class);
        when(rand.nextLong()).thenReturn((long)150);

        DistributionManager distr1=new DistributionManager(rand, 1,1);
        distr1.setDatabaseSystem(databaseSystem_A);
        distr1.setExistingServers(list);
        databaseSystem_A.setDistributionManager(distr1);

        DistributionManager distr2=new DistributionManager(1,1);
        distr2.setDatabaseSystem(databaseSystem_B);
        distr2.setExistingServers(list);
        databaseSystem_B.setDistributionManager(distr2);

        // 2. Insert 2 images in B
        // 2.1 We create a connection from the client
        DatabaseConnection connection=new DatabaseConnection(
                allServers, mockHashingFunction, new ClientTools(), new Random());

        // 2.3 We insert the images
        connection.insert(str1);
        connection.insert(str2);

        // we check that before the moving the tag are not in A:
        Assert.assertNull(databaseSystem_A.getLocalDataSystem().getImageHashes("fox"));

        // 3. Get new virtual tag node in A
        // (The new virtual node has range 100-150)
        databaseSystem_A.getDistributionManager().attachNewTagVirtualNode();

        // 4. Assert that the tags were copied in A
        Assert.assertEquals((long) 167, (long) databaseSystem_A.getLocalDataSystem().getImageHashes("fox").getFirst());
        Assert.assertEquals((long)172, (long)databaseSystem_A.getLocalDataSystem().getImageHashes("tiger").getFirst() );
        Assert.assertEquals((long)167, (long)databaseSystem_A.getLocalDataSystem().getImageHashes("snow").getFirst() );
        Assert.assertEquals((long) 172, (long) databaseSystem_A.getLocalDataSystem().getImageHashes("snow").get(1));

        // We tell the database servers to close their listening threads
        CCloseThread command= new CCloseThread(databaseSystem_A.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(databaseSystem_B.getServerNetworkInfo());
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
