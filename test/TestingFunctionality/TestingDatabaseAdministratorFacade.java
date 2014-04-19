package TestingFunctionality;

import ConsistentHashing.HelpingClasses.HashingFunction;
import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import Interface.Database;
import Interface.DatabaseAdministratorFacade;
import Interface.DatabaseConnection;
import junit.framework.Assert;
import NetworkInfrastructure.NetworkCommands.CCloseThread;
import NetworkInfrastructure.ServerNetworkInfo;
import org.apache.commons.io.FileUtils;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/20/13
 * Time: 10:52 PM
 */
public class TestingDatabaseAdministratorFacade {
    @org.junit.Test
    public void testCreateNew() throws IOException {
        // it creates a database and it connects to it. After this it inserts
        // 3 images and searches for them, and deletes one of them.


        // we create a database
        String rootDirectory="test/DemoStorage/";
        DatabaseSystem database =
                new DatabaseAdministratorFacade().createNew(
                        8100, 3, 3, 5, 5, rootDirectory);

        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8100));


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


        // 3. We insert the images
        connection.insert(str1);
        connection.insert(str2);
        connection.insert(str3);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // We search for images in the database
        LinkedList<String> tags=new LinkedList<String>();
        tags.add("tiger");
        LinkedList<ImageWithMetadata> returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));

        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str1));
        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));


        // We delete an image str1 from the database

        boolean result=connection.delete(str1);
        Assert.assertTrue(result);

        // We search again for image "str1" in the database
        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);
        // And we assert that we don't find it now
        Assert.assertEquals(false, returnedList.contains(str1));

        // We tell the database servers to close their listening threads
        CCloseThread command= new CCloseThread(database.getServerNetworkInfo());
        command.request();
    }

    @org.junit.Test
    public void testConnectServerToDatabase() throws IOException {
        // it creates a database and with another servers it connects to it

        DatabaseAdministratorFacade facade = new DatabaseAdministratorFacade();
        // we create a database
        String rootDirectory="test/DemoStorage/1/";
        DatabaseSystem database1 =facade.createNew( 8300, 2, 2, 5, 5, rootDirectory);

        DatabaseSystem database2 =facade.connectServerToDatabase(
                        new ServerNetworkInfo("127.0.0.1", 8300),
                            8301, 5, 5, "test/DemoStorage/2/");

        DatabaseSystem database3 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8301),
                8302, 5, 5, "test/DemoStorage/3/");

        DatabaseSystem database4 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8300),
                8303, 5, 5, "test/DemoStorage/4/");

        DatabaseSystem database5 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8302),
                8304, 5, 5, "test/DemoStorage/5/");



        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8302));


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


        // 3. We insert the images
        Assert.assertTrue( connection.insert(str1) );
        Assert.assertTrue( connection.insert(str2) );
        Assert.assertTrue( connection.insert(str3) );

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // We search for images in the database
        LinkedList<String> tags=new LinkedList<String>();
        tags.add("tiger");
        LinkedList<ImageWithMetadata> returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));

        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);

        Assert.assertEquals(true, returnedList.contains(str1));
        Assert.assertEquals(true, returnedList.contains(str2));
        Assert.assertEquals(true, returnedList.contains(str3));


        // We delete image str1 from the database
        boolean result=connection.delete(str1);
        Assert.assertTrue(result);

        // We search again for image "str1" in the database
        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);
        // And we assert that we don't find it now
        Assert.assertEquals(false, returnedList.contains(str1));


        // We tell the database servers to close their listening threads
        CCloseThread command= new CCloseThread(database1.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(database2.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(database3.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(database4.getServerNetworkInfo());
        command.request();
        command= new CCloseThread(database5.getServerNetworkInfo());
        command.request();
    }

    @org.junit.Test
    public void testDisconnectServerFromDatabase() throws IOException {
        // it creates a database and with another servers it connects to it
        // then it disconnects and we assert that we didn't lost any data

        DatabaseAdministratorFacade facade = new DatabaseAdministratorFacade();
        // we create a database
        String rootDirectory="test/DemoStorage/1/";
        DatabaseSystem database1 =facade.createNew( 8300, 3, 3, 5, 5, rootDirectory);

        DatabaseSystem database2 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8300),
                8301, 5, 5, "test/DemoStorage/2/");


        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8301));


        // 1. We create the images to be inserted
        String imagesFolder="test/ImagesData/";
        ImageWithMetadata str1= new ImageWithMetadata();
        str1.fileName="fox snow web.jpg";
        String path=imagesFolder+str1.fileName;
        str1.raw= Files.readAllBytes(Paths.get(path));
        str1.tags.add("fox");
        str1.tags.add("snow");


        // 3. We insert the image
        Assert.assertTrue( connection.insert(str1) );

        // 4. We disconnect the server with the first image str1
        DatabaseSystem d=null;
        ServerNetworkInfo newInfo=null;

        long hash=new HashingFunction().getImageHash(str1);
        if( database1.getLocalDataSystem().getImage(hash) !=null) {
            newInfo=new ServerNetworkInfo("127.0.0.1", 8301);
            d=database1;
        }
        if(database2.getLocalDataSystem().getImage(hash) !=null) {
            newInfo=new ServerNetworkInfo("127.0.0.1", 8300);
            d=database2;
        }
        new DatabaseAdministratorFacade().disconnectServerFromDatabase(d);

        // 5 We assert that we can still find image str1
        connection =new Database().connectTo(newInfo);

        LinkedList<String> tags=new LinkedList<String>();
        tags.add("fox");
        LinkedList<ImageWithMetadata> returnedList = connection.get(tags);
        Assert.assertEquals(returnedList.getFirst(), str1);

        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);
        Assert.assertEquals(returnedList.getFirst(), str1);

        // We tell the database servers to close their listening threads
        if(database1.getIncomingConnectionsThread().isThreadAlive()) {
            CCloseThread command= new CCloseThread(database1.getServerNetworkInfo());
            command.request();
        }
        if(database2.getIncomingConnectionsThread().isThreadAlive()) {
            CCloseThread command= new CCloseThread(database2.getServerNetworkInfo());
            command.request();
        }
    }

    @org.junit.Test
    public void testBackUpClose_and_Restore() throws IOException {
        //1. it creates a database
        //2.inserts an image
        //3.  back-up the data
        //4. close the database
        //5. create a new database
        //6. restore data from backup
        //7. assert that the image still exists and it's the same with the original

        //1. it creates a database
        DatabaseAdministratorFacade facade = new DatabaseAdministratorFacade();
        String rootDirectory="test/DemoStorage/1/";
        DatabaseSystem database1 =facade.createNew( 8300, 1, 1, 5, 5, rootDirectory);

        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8300));


        // 1. We create the image to be inserted
        String imagesFolder="test/ImagesData/";
        ImageWithMetadata str1= new ImageWithMetadata();
        str1.fileName="fox snow web.jpg";
        String path=imagesFolder+str1.fileName;
        str1.raw= Files.readAllBytes(Paths.get(path));
        str1.tags.add("fox");
        str1.tags.add("snow");

        // 2. We insert the image
        Assert.assertTrue( connection.insert(str1) );

        //3.  back-up the data
        String backUpDir="test/ImagesData/backUp/";
        new File(backUpDir).mkdirs();
        facade.backUpData(database1,backUpDir);

        //4. close the database
        facade.close(database1);

        //5. create a new database
        rootDirectory="test/DemoStorage/2/";
        DatabaseSystem database2 =facade.createNew( 8301, 1, 1, 5, 5, rootDirectory);

        //6. restore data from backup
        facade.insertDataFromBackUp(database2.getServerNetworkInfo(),"test/ImagesData/backUp/");

        //7. assert that the image still exists and it's the same with the original
        connection = new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8301));


        LinkedList<String> tags=new LinkedList<String>();
        tags.add("fox");
        LinkedList<ImageWithMetadata> returnedList = connection.get(tags);
        Assert.assertEquals(returnedList.getFirst(), str1);

        tags=new LinkedList<String>();
        tags.add("snow");
        returnedList = connection.get(tags);
        Assert.assertEquals(returnedList.getFirst(), str1);

        // we delete the node
        File backDir=new File(backUpDir);
        FileUtils.deleteDirectory(backDir);

        facade.close(database2);
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
