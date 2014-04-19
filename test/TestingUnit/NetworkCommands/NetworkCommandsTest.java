package TestingUnit.NetworkCommands;

import ConsistentHashing.DistributionManager;
import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.HashingFunction;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import Data.LocalDataSystem;
import NetworkInfrastructure.IncomingConnectionsThread;
import NetworkInfrastructure.NetworkCommands.CGetServersAddresses;
import NetworkInfrastructure.NetworkCommands.CPing;
import NetworkInfrastructure.NetworkCommands.CUpdateServersRanges;
import NetworkInfrastructure.NetworkCommands.Image.*;
import NetworkInfrastructure.NetworkCommands.Tag.*;
import NetworkInfrastructure.ServerNetworkInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/30/13
 * Time: 12:32 PM
 */

public class NetworkCommandsTest {

    private static ServerNetworkInfo serverNetworkInfo;
    private static DatabaseSystem databaseSystem;
    private static IncomingConnectionsThread incomingConnectionsThread;
    static private int initialPort=8200;

    @Before
    public void setUp() {        // Initialize stuff before every test
        // creating the server
        serverNetworkInfo=mock(ServerNetworkInfo.class);
        when(serverNetworkInfo.getIP()).thenReturn("127.0.0.1");
        when(serverNetworkInfo.getPort()).thenReturn(8200);
        when(serverNetworkInfo.toString()).thenReturn("127.0.0.1:"+initialPort++);

        databaseSystem = mock(DatabaseSystem.class);
        when(databaseSystem.getServerNetworkInfo()).thenReturn(serverNetworkInfo);

        incomingConnectionsThread=spy(new IncomingConnectionsThread(databaseSystem));
        when(incomingConnectionsThread.isThreadAlive()).thenReturn(true).thenReturn(false);
        Thread thread = new Thread(incomingConnectionsThread);
        thread.start();
    }


    @Test
    public void testCRequestRawData_goodScenario() throws IOException {

        String path1="test/ImagesData/the_tiger_and_the_snow.jpg";
        String path2="test/ImagesData/Siberian_Tiger_2.jpg";

        //images to be transferred
        LinkedList<String> tags = new LinkedList<String>();
        tags.add("tiger");
        tags.add("winter");
        ImageWithMetadata img1=new ImageWithMetadata("test1",null,tags);
        img1.raw= Files.readAllBytes(Paths.get(path1));
        ImageWithMetadata img2=new ImageWithMetadata("test2",null,tags);
        img2.raw= Files.readAllBytes(Paths.get(path2));
        LinkedList<ImageWithMetadata> imageList=new LinkedList<ImageWithMetadata>();
        imageList.add(img1);
        imageList.add(img2);
        HashRange h = new HashRange(30, 120);

        // creating the server
        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.getAllImagesInRange(Matchers.any(HashRange.class))).thenReturn(imageList);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);

        //creating the client
        LocalDataSystem LocalDataSystemMock=mock(LocalDataSystem.class);
        when(LocalDataSystemMock.insertImage(any(ImageWithMetadata.class))).thenReturn(true);

        DatabaseSystem clientD = mock(DatabaseSystem.class);
        when(clientD.getLocalDataSystem()).thenReturn(LocalDataSystemMock);

        // creating and executing the command
        CRequestRawData command=new CRequestRawData(h,serverNetworkInfo,clientD);
        command.request();   // we execute the command

        // checking results
        ArgumentCaptor<ImageWithMetadata> argument =
                ArgumentCaptor.forClass(ImageWithMetadata.class);
        verify(LocalDataSystemMock, times(2)).insertImage(argument.capture());
        List<ImageWithMetadata> arguments = argument.getAllValues();

        if (arguments.get(0).fileName.equals(img1.fileName) ) {
            Assert.assertArrayEquals(arguments.get(0).toBytes(), img1.toBytes());
            Assert.assertArrayEquals(arguments.get(1).toBytes(), img2.toBytes());
        }
        else {
            Assert.assertArrayEquals(arguments.get(1).toBytes(), img1.toBytes());
            Assert.assertArrayEquals(arguments.get(0).toBytes(), img2.toBytes());
        }
    }

    @Test
    public void testCInsertImage_goodScenario() throws IOException {

        //image to be transferred
        LinkedList<String> tags = new LinkedList<String>();
        tags.add("tiger");
        tags.add("winter");
        ImageWithMetadata img=new ImageWithMetadata("test1",null,tags);

        String path="test/ImagesData/the_tiger_and_the_snow.jpg";
        img.raw= Files.readAllBytes(Paths.get(path));


        // creating the server
        LocalDataSystem LocalDataSystem=mock(LocalDataSystem.class);
        ArgumentCaptor<ImageWithMetadata> argument =
                ArgumentCaptor.forClass(ImageWithMetadata.class);
        when(LocalDataSystem.insertImage(argument.capture())).thenReturn(true);
        when(databaseSystem.getLocalDataSystem()).thenReturn(LocalDataSystem);

        // the client
        CInsertImage command=new CInsertImage(serverNetworkInfo, img);

        boolean response=command.request();   // we execute the command
        Assert.assertEquals(true,response);

        // We verify that the received image is the same
        Assert.assertArrayEquals(img.toBytes(), argument.getValue().toBytes());
    }

    @Test
    public void testCGetImage_goodScenario() throws IOException {

        //images to be transferred
        LinkedList<String> tags = new LinkedList<String>();
        tags.add("tiger");
        tags.add("winter");
        ImageWithMetadata img=new ImageWithMetadata("test1", null, tags);
        String path="test/ImagesData/the_tiger_and_the_snow.jpg";
        img.raw= Files.readAllBytes(Paths.get(path));

        // creating the server
        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.getImage(100)).thenReturn(img);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);

        //creating the client

        // creating and executing the command
        CGetImage command=new CGetImage(serverNetworkInfo,100);
        boolean result=command.request(); // we execute the command
        Assert.assertEquals(true, result);

        // checking results
        Assert.assertArrayEquals(img.toBytes(),
                command.getImageWithMetadata().toBytes());
    }

    @Test
    public void testCDeleteImage_goodScenario() throws IOException {

        //tags to be returned
        LinkedList<String> tags = new LinkedList<String>();
        tags.add("fox");
        tags.add("winter");

        // creating the server
        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.deleteImage(100)).thenReturn(tags);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);

        //creating the client

        // creating and executing the command
        CDeleteImage command=new CDeleteImage(serverNetworkInfo,100);
        boolean result=command.request(); // we execute the command
        Assert.assertEquals(true, result);

        // checking results
        Assert.assertArrayEquals(tags.toArray(), command.getTags().toArray());
    }

    @Test
    public void testCInsertTag_goodScenario() throws IOException {

        //tags to be inserted
         String tag="sunset";
         long imageValueHash=130;

        // creating the server
        LocalDataSystem LocalDataSystem=mock(LocalDataSystem.class);

        when(LocalDataSystem.insertTag(eq(tag),eq(imageValueHash))).thenReturn(true);
        when(databaseSystem.getLocalDataSystem()).thenReturn(LocalDataSystem);

        // the client
        CInsertTag command=new CInsertTag(serverNetworkInfo, tag, imageValueHash);

        boolean response=command.request();   // we execute the command
        Assert.assertEquals(true,response);
    }

    @Test
    public void testCDeleteTag_goodScenario() throws IOException {

        //tags to be inserted
        String tag="sunset";
        long tagHash=new HashingFunction().getTagHash(tag);
        long imageValueHash=130;

        // creating the server
        LocalDataSystem LocalDataSystem=mock(LocalDataSystem.class);
        when( LocalDataSystem.deleteTag( tagHash, imageValueHash )).thenReturn(true);
        when( databaseSystem.getLocalDataSystem() ).thenReturn(LocalDataSystem);

        // the client
        CDeleteTag command=new CDeleteTag(serverNetworkInfo, tagHash, imageValueHash);

        boolean response=command.request();   // we execute the command
        Assert.assertEquals(true,response);
    }

    @Test
    public void testCGetImageHashCollectionByTag_goodScenario() throws IOException {

        //imageHashes to be returned
        LinkedList<Long> list = new LinkedList<Long>();
        list.add((long)100);
        list.add((long)103);
        list.add((long)12);

        // creating the server
        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.getImageHashes("fox")).thenReturn(list);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);

        //creating the client

        // creating and executing the command
        CGetImageHashCollectionByTag command=
                new CGetImageHashCollectionByTag(serverNetworkInfo,"fox");
        boolean result=command.request();  // we execute the command
        Assert.assertEquals(true, result);

        // checking results
        Assert.assertArrayEquals(list.toArray(), command.getImagesHashList().toArray());
    }

    @Test
    public void testCGetServersResponsibleForImageHash_goodScenario()
            throws IOException {

        //Network Info to be transferred
        LinkedList<ServerNetworkInfo> list=new LinkedList<ServerNetworkInfo>();
        ServerNetworkInfo requestedServer=new ServerNetworkInfo(
                "127.0.0.1",5678
        );
        list.add(requestedServer);

        // creating the server
        DistributionManager mockDistributionManage=mock(DistributionManager.class);
        when(mockDistributionManage.getServersResponsibleForImageHash(100)).
                thenReturn(list);
        when(databaseSystem.getDataDistributionManager()).thenReturn(mockDistributionManage);

        //creating the client

        // creating and executing the command
        CGetServersResponsibleForImageHash command=
                new CGetServersResponsibleForImageHash(serverNetworkInfo,100);
        boolean result=command.request(); // we execute the command
        Assert.assertEquals(true, result);


        // checking results
        Assert.assertEquals(true, requestedServer.toString().equals(
                command.getServersRequested().getFirst().toString()));
    }

    @Test
    public void testCGetServersResponsibleForTagHash_goodScenario()
            throws IOException {

        //Network Info to be transferred
        LinkedList<ServerNetworkInfo> list=new LinkedList<ServerNetworkInfo>();
        ServerNetworkInfo requestedServer=new ServerNetworkInfo(
                "127.0.0.1",5678
        );
        list.add(requestedServer);

        // creating the server
        DistributionManager mockDistributionManage=mock(DistributionManager.class);
        when(mockDistributionManage.getServersResponsibleForTagHash(100)).
                thenReturn(list);
        when(databaseSystem.getDataDistributionManager()).thenReturn(mockDistributionManage);

        //creating the client

        // creating and executing the command
        CGetServersResponsibleForTagHash command=
                new CGetServersResponsibleForTagHash(serverNetworkInfo,100);
        boolean result=command.request(); // we execute the command
        Assert.assertEquals(true, result);

        // checking results
        Assert.assertEquals(true, requestedServer.toString().equals(
                command.getServersRequested().getFirst().toString() ));
    }


    @Test
    public void testCGetServersAddresses_goodScenario()
            throws IOException {

        //Network Info to be transferred
        ServerNetworkInfo requestedServer1=new ServerNetworkInfo("127.0.0.1",5678);
        ServerNetworkInfo requestedServer2=new ServerNetworkInfo("127.0.0.2",1248);
        ServerNetworkInfo requestedServer3=new ServerNetworkInfo("127.0.1.1",1921);
        LinkedList<ServerNetworkInfo> list=new LinkedList<ServerNetworkInfo>();
        list.add(requestedServer1);
        list.add(requestedServer2);
        list.add(requestedServer3);

        // creating the server
        DistributionManager mockDistributionManage=mock(DistributionManager.class);
        when(mockDistributionManage.getServersAddresses()).thenReturn(list);
        when(databaseSystem.getServerDistributionManager()).thenReturn(mockDistributionManage);

        //creating the client

        // creating and executing the command
        CGetServersAddresses command=
                new CGetServersAddresses(serverNetworkInfo);
       command.request(); // we execute the command

        // checking results
        Assert.assertArrayEquals(list.toArray(),
                command.getServersRequested().toArray());
    }

    @Test
    public void testCRequestRawTags_goodScenario() throws IOException {

        //images to be transferred
        LinkedList<Long> tagHashes = new LinkedList<Long>();
        tagHashes.add((long)100);
        tagHashes.add((long)200);

        LinkedList<Long> imageHashes1 = new LinkedList<Long>();
        imageHashes1.add((long)15);
        imageHashes1.add((long)25);

        LinkedList<Long> imageHashes2 = new LinkedList<Long>();
        imageHashes2.add((long)30);

        // creating the server
        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.getAllTagsInRange(Matchers.any(HashRange.class))).thenReturn(tagHashes);
        when(mockLocalDataSystem.getImageHashes(100)).thenReturn(imageHashes1);
        when(mockLocalDataSystem.getImageHashes(200)).thenReturn(imageHashes2);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);


        //creating the client
        LocalDataSystem LocalDataSystemMock=mock(LocalDataSystem.class);
        when(LocalDataSystemMock.insertTag(anyLong(),anyLong())).thenReturn(true);

        DatabaseSystem clientD = mock(DatabaseSystem.class);
        when(clientD.getLocalDataSystem()).thenReturn(LocalDataSystemMock);

        // creating and executing the command
        HashRange h=new HashRange(0,300);
        CRequestRawTags command=new CRequestRawTags(h,serverNetworkInfo,clientD);
        command.request();   // we execute the command

        // checking results
        ArgumentCaptor<Long> argument1 =ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> argument2 =ArgumentCaptor.forClass(Long.class);

        verify(LocalDataSystemMock, times(3)).insertTag(argument1.capture(), argument2.capture());
        List<Long> arguments1 = argument1.getAllValues();
        List<Long> arguments2= argument2.getAllValues();

        Assert.assertEquals((long)arguments1.get(0), (long)100);
        Assert.assertEquals((long)arguments2.get(0), (long)15);
        Assert.assertEquals((long)arguments1.get(1), (long)100);
        Assert.assertEquals((long)arguments2.get(1), (long)25);
        Assert.assertEquals((long)arguments1.get(2), (long)200);
        Assert.assertEquals((long)arguments2.get(2), (long)30);
    }


    @Test
    public void testCPing_goodScenario() throws IOException {
        //creating the client

        // creating and executing the command
        CPing command=new CPing(serverNetworkInfo);
        boolean result=command.request(); // we execute the command
        Assert.assertEquals(true, result);
    }

    @Test
    public void testCUpdateServersRanges() throws IOException {
        // setting the client side
        LinkedList<ServerSegmentsStruct> newServerRanges=
                new LinkedList<ServerSegmentsStruct>();
        LinkedList<HashRange> list = new LinkedList<HashRange>();
        list.add(new HashRange(75,100));
        newServerRanges.add(
                new ServerSegmentsStruct(list,list,serverNetworkInfo));



        // setting the server side
        // setting the old segments (0-100);
        ArgumentCaptor<HashRange> argument =
                ArgumentCaptor.forClass(HashRange.class);
        LinkedList<HashRange> dataRanges=new LinkedList<HashRange>();
        dataRanges.add(new HashRange(0,100));

        LocalDataSystem mockLocalDataSystem=mock(LocalDataSystem.class);
        when(mockLocalDataSystem.getDataRanges()).thenReturn(dataRanges);
        when(mockLocalDataSystem.getTagRanges()).thenReturn(dataRanges);

        when(databaseSystem.getServerNetworkInfo()).thenReturn(serverNetworkInfo);
        when(databaseSystem.getLocalDataSystem()).thenReturn(mockLocalDataSystem);

        DistributionManager mockDistributionManager= mock(DistributionManager.class);
        when(databaseSystem.getDistributionManager()).thenReturn(mockDistributionManager);


        // making the command
        CUpdateServersRanges command=
                new CUpdateServersRanges (serverNetworkInfo,newServerRanges);
        command.request();   // we execute the command

        // verifying interactions
        try{
            Thread.currentThread().sleep(200); // we make it wait a little to make
            // sure that we sent the command through the socket
        } catch (InterruptedException e) {        }
        verify(mockDistributionManager).setExistingServers(newServerRanges);
        verify(mockLocalDataSystem).deleteAllImagesInRange(new HashRange(0,75));
        verify(mockLocalDataSystem).deleteAllITagsInRange(new HashRange(0,75));
    }
}