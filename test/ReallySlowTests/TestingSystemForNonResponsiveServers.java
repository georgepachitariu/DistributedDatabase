package ReallySlowTests;

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
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Random;


public class TestingSystemForNonResponsiveServers {
    @org.junit.Test
    public void testNonResponsiveServer() throws IOException {
        // it creates a database and with three servers
        // then it stops one server and we assert that we didn't lost any data

        DatabaseAdministratorFacade facade = new DatabaseAdministratorFacade();
        // we create a database
        String rootDirectory="test/DemoStorage/1/";
        DatabaseSystem database1 =facade.createNew( 8300, 2, 2, 3, 3, rootDirectory);

        DatabaseSystem database2 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8300),
                8301, 3, 3, "test/DemoStorage/2/");

        DatabaseSystem database3 =facade.connectServerToDatabase(
                new ServerNetworkInfo("127.0.0.1", 8301),
                8302, 3, 3, "test/DemoStorage/3/");


        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8300));

        // here we simulate activity on the database
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Random r=new Random();
                    DatabaseConnection connection =
                            new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8300));
                    for(int i=0;i<100000;i++) {
                        LinkedList<String> list=new LinkedList<String>();
                        list.add(String.valueOf(r.nextInt()));
                        connection.get(list);
                        System.out.println(1234);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        // 1. We create the images to be inserted
        String imagesFolder="test/ImagesData/";
        ImageWithMetadata str1= new ImageWithMetadata();
        str1.fileName="fox snow web.jpg";
        String path=imagesFolder+str1.fileName;
        str1.raw= Files.readAllBytes(Paths.get(path));
        str1.tags.add("fox");
        str1.tags.add("snow");

        // 3. We insert the image
        Assert.assertTrue(connection.insert(str1));




        // 4. We stop one of the servers (all the images are in duplicates
        // on 2 servers) with the  image str1
        CCloseThread command=null;
        long imageHash=new HashingFunction().getImageHash(str1);
        if( database2.getLocalDataSystem().getImage(imageHash) !=null) {
            ServerNetworkInfo newInfo=new ServerNetworkInfo("127.0.0.1", 8301);
            command = new CCloseThread(newInfo);
        }
        if(database3.getLocalDataSystem().getImage(imageHash) !=null) {
            ServerNetworkInfo newInfo=new ServerNetworkInfo("127.0.0.1", 8302);
            command = new CCloseThread(newInfo);
        }
        command.request();


        // we wait a short period of time (for the database to realise of the broken
        // server and redistribute)
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // 5 We assert that we can still find image str1 on both remaining servers
        Assert.assertTrue(str1.equals(  database1.getLocalDataSystem().getImage(imageHash)  ));
        if(database2.getIncomingConnectionsThread().isThreadAlive())
            Assert.assertTrue(str1.equals(  database2.getLocalDataSystem().getImage(imageHash)  ));
        if(database3.getIncomingConnectionsThread().isThreadAlive())
            Assert.assertTrue(str1.equals(  database3.getLocalDataSystem().getImage(imageHash)  ));



        // We tell the database servers to close their listening threads
        command= new CCloseThread(database1.getServerNetworkInfo());
        command.request();
        if(database2.getIncomingConnectionsThread().isThreadAlive()) {
             command= new CCloseThread(database2.getServerNetworkInfo());
            command.request();
        }
        if(database3.getIncomingConnectionsThread().isThreadAlive()) {
             command= new CCloseThread(database3.getServerNetworkInfo());
            command.request();
        }
    }

    @Before
    public void cleaningMethod()  {
        try {

        // delete sub-Folders
        String folderName="test/DemoStorage/";
        File[] databaseFolders=new File(folderName).listFiles();
        for(File f :databaseFolders)
            FileUtils.deleteDirectory(f);

        } catch (SocketTimeoutException s) {

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
