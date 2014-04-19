package Interface;

import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import NetworkInfrastructure.NetworkCommands.CCloseThread;
import NetworkInfrastructure.NetworkCommands.CGetExistingServersSegments;
import NetworkInfrastructure.NetworkCommands.CMoveNode;
import NetworkInfrastructure.NetworkCommands.CUpdateServersRanges;
import NetworkInfrastructure.ServerNetworkInfo;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/17/13
 * Time: 3:03 PM
 */
public class DatabaseAdministratorFacade {


    public DatabaseSystem createNew(int localPort,
                                    int imageDuplicationNr, int tagDuplicationNr,
                                    int imageVIrtualNodesNr, int tagVIrtualNodesNr,
                                    String rootDirectory
    ) {
        ServerNetworkInfo current= new ServerNetworkInfo("127.0.0.1", localPort);
        DatabaseSystem databaseSystem =
                new DatabaseSystem(current, rootDirectory, imageDuplicationNr,  tagDuplicationNr);
        databaseSystem.sendReferenceToDistributionManager();
        databaseSystem.startIncomingConnectionsThread();

        // we add the virtual nodes
        for(int i=0; i<imageVIrtualNodesNr; i++)
            databaseSystem.getServerDistributionManager().attachNewDataVirtualNode();
        for(int i=0; i<tagVIrtualNodesNr; i++)
            databaseSystem.getServerDistributionManager().attachNewTagVirtualNode();

        return databaseSystem;
    }

    public void close(DatabaseSystem d) {
        try {
            // close the "IncomingConnections" Thread
            CCloseThread command= new CCloseThread(d.getServerNetworkInfo());
            command.request();

            // delete sub-Folders
            String folderName=d.getLocalDataSystem().getRootDirectory();
            File[] databaseFolders=new File(folderName).listFiles();
            for(File f :databaseFolders)
                FileUtils.deleteDirectory(f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void backUpData(DatabaseSystem d, String backupLocation) {
        LinkedList<ImageWithMetadata> imageListd=new LinkedList<ImageWithMetadata>();
        for(HashRange h : d.getLocalDataSystem().getDataRanges())
            imageListd.addAll(d.getLocalDataSystem().getAllImagesInRange(h));

        for(ImageWithMetadata im : imageListd) {
            im.writeToDisk(backupLocation+"/" + im.fileName);
        }
    }

    public void insertDataFromBackUp(ServerNetworkInfo databaseAdress, String backupLocation) throws IOException {
        // we create a connection with the database
        DatabaseConnection connection = new Database().connectTo(databaseAdress);

        File rootFolder = new File(backupLocation);
        LinkedList<ImageWithMetadata> returnList= new LinkedList<ImageWithMetadata>();
        File[] imageList = rootFolder.listFiles();
        for( File f : imageList )
            connection.insert(new ImageWithMetadata(f.toPath()));
    }

    public DatabaseSystem connectServerToDatabase(ServerNetworkInfo databaseAdress, int localPort,
                                                  int imageVIrtualNodesNr,int tagVIrtualNodesNr, String rootDirectory) throws IOException {
        CGetExistingServersSegments command=new CGetExistingServersSegments(databaseAdress);
        command.request();

        LinkedList<ServerSegmentsStruct> existingServers = command.getServersRequested();
        int imgDuplicationNr=command.getImageDuplicationNr();
        int tagDuplicationNr=command.getTagDuplicationNr();

        ServerNetworkInfo localAddress= new ServerNetworkInfo("127.0.0.1", localPort);
        DatabaseSystem databaseSystem =
                new DatabaseSystem(localAddress, rootDirectory, imgDuplicationNr,  tagDuplicationNr,existingServers);
        databaseSystem.sendReferenceToDistributionManager();
        databaseSystem.startIncomingConnectionsThread();

        // we add the virtual nodes
        for(int i=0; i<imageVIrtualNodesNr; i++)
            databaseSystem.getServerDistributionManager().attachNewDataVirtualNode();
        for(int i=0; i<tagVIrtualNodesNr; i++)
            databaseSystem.getServerDistributionManager().attachNewTagVirtualNode();

        // we update the other servers
        for(ServerSegmentsStruct s : existingServers)
            if(! s.serverNetworkInfo .equals( databaseSystem.getServerNetworkInfo())){
                CUpdateServersRanges cmd=new CUpdateServersRanges(s.serverNetworkInfo,
                        databaseSystem.getDistributionManager().getExistingServers());
                cmd.request();
            }
        return databaseSystem;
    }

    public void disconnectServerFromDatabase(DatabaseSystem databaseSystem) throws IOException {
        LinkedList<ServerSegmentsStruct> existingServers =
                databaseSystem.getDistributionManager().getExistingServers();
        ServerSegmentsStruct serverToBeRemoved = databaseSystem.getServerRanges();

        if(existingServers.size() > databaseSystem.getDistributionManager().getImageDuplicationNr()) {
            // we only move nodes if nr_of_existing_Servers> nr_of_duplicates
            LinkedList<HashRange> dataRanges = serverToBeRemoved.dataRanges;
            while(dataRanges.size()>0)
                for(ServerSegmentsStruct server : existingServers )
                    if(! server.serverNetworkInfo.equals(serverToBeRemoved.serverNetworkInfo)) {
                        CMoveNode cmd=new CMoveNode(server.serverNetworkInfo,dataRanges.pop(),true);
                        cmd.request();
                        if(dataRanges.size()==0) break;
                    }
        }

        if(existingServers.size() > databaseSystem.getDistributionManager().getTagDuplicationNr()) {
            // we only move nodes if nr_of_existing_Servers> nr_of_duplicates
            LinkedList<HashRange> tagRanges = serverToBeRemoved.tagRanges;
            while(tagRanges.size()>0)
                for(ServerSegmentsStruct server : existingServers ) {
                    if(! server.serverNetworkInfo .equals( serverToBeRemoved.serverNetworkInfo)) {
                        CMoveNode cmd=new CMoveNode(server.serverNetworkInfo,tagRanges.pop(), false);
                        cmd.request();
                        if(tagRanges.size()==0) break;
                    }
                }
        }

        // we remove the current server from the "existingServer' and we update the remaining servers of the change
        LinkedList<ServerSegmentsStruct> segList = existingServers;
        for(int i=0;i<segList.size(); i++)
            if(segList.get(i).serverNetworkInfo.equals(serverToBeRemoved.serverNetworkInfo)) {
                segList.remove(i);
                break;
            }
        databaseSystem.getDistributionManager().setExistingServers(segList);
        databaseSystem.getDistributionManager().updateRangesOnAllServers();

        new CCloseThread(databaseSystem.getServerNetworkInfo()).request();

        // finally we delete the files and folders
        String folderName=databaseSystem.getLocalDataSystem().getRootDirectory();
        File[] databaseFolders=new File(folderName).listFiles();
        for(File f :databaseFolders)
            FileUtils.deleteDirectory(f);
    }
}
