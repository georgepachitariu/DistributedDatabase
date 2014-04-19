package TestingFunctionality;

import Data.DatabaseSystem;
import Interface.Database;
import Interface.DatabaseAdministratorFacade;
import Interface.DatabaseConnection;
import networkInfrastructure.ServerNetworkInfo;
import org.apache.commons.io.FileUtils;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/27/13
 * Time: 11:57 AM
 */
public class TestingSystemForNonResponsiveServers {
    @org.junit.Test
    public void testNonResponsiveServer() throws IOException {
        // it creates a database and with three servers
        // then it stops one server and we assert that we didn't lost any data

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


        // we create a connection to it:
        DatabaseConnection connection =
                new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8301));
/*
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Random r=new Random();
                    DatabaseConnection connection =
                            new Database().connectTo(new ServerNetworkInfo("127.0.0.1", 8301));
                    while(true) {
                        LinkedList<String>list=new LinkedList<String>();
                        list.add(String.valueOf(r.nextInt()));
                        connection.get(list);
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



        // 4. We stop the server with the first image str1
        DatabaseSystem d=null;
        ServerNetworkInfo newInfo=null;

        long hash=new HashingFunction().getImageHash(str1);
        if( database2.getLocalDataSystem().getImage(hash) !=null) {
            newInfo=new ServerNetworkInfo("127.0.0.1", 8301);
            d=database2;
        }
        if(database3.getLocalDataSystem().getImage(hash) !=null) {
            newInfo=new ServerNetworkInfo("127.0.0.1", 8302);
            d=database3;
        }
        CCloseThread command= new CCloseThread(d.getServerNetworkInfo());
        command.request();




        while(true);*/

/*
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
        command= new CCloseThread(database1.getServerNetworkInfo());
        command.request();
        if(database2.getIncomingConnectionsThread().isThreadAlive()) {
             command= new CCloseThread(database2.getServerNetworkInfo());
            command.request();
        }
        if(database3.getIncomingConnectionsThread().isThreadAlive()) {
             command= new CCloseThread(database3.getServerNetworkInfo());
            command.request();
        }*/
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
