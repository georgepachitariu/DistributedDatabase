package Data;

import ConsistentHashing.DataDistributionManager;
import ConsistentHashing.DistributionManager;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import ConsistentHashing.ServerDistributionManager;
import NetworkInfrastructure.IncomingConnectionsThread;
import NetworkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/28/13
 * Time: 4:12 PM
 */
public class DatabaseSystem {

    private ServerNetworkInfo serverNetworkInfo ;
    private LocalDataSystem LocalDataSystem;
    private DistributionManager distributionManager;
    private IncomingConnectionsThread incomingConnectionsThread;

    public DatabaseSystem(ServerNetworkInfo serverNetworkInfo,
                          String rootDirectory,int imageDuplicationNr, int tagDuplicationNr) {
        this(serverNetworkInfo ,new LocalDataSystem(rootDirectory),
                new DistributionManager(imageDuplicationNr, tagDuplicationNr)
        );
    }

    public DatabaseSystem(ServerNetworkInfo serverNetworkInfo,
                          String rootDirectory,int imageDuplicationNr, int tagDuplicationNr,
                          LinkedList<ServerSegmentsStruct> existingServers) {
        this(serverNetworkInfo ,new LocalDataSystem(rootDirectory),
                new DistributionManager(imageDuplicationNr, tagDuplicationNr, existingServers)
        );
    }

    public DatabaseSystem(ServerNetworkInfo serverNetworkInfo,
                          LocalDataSystem localDataSystem,
                          DistributionManager distributionManager
    ) {
        this.serverNetworkInfo=serverNetworkInfo;
        this.serverNetworkInfo = serverNetworkInfo;
        this.LocalDataSystem = localDataSystem;
        this.distributionManager = distributionManager;
    }

    public void sendReferenceToDistributionManager() {
        this.distributionManager.setDatabaseSystem(this);
    }

    public void startIncomingConnectionsThread() {
        this.incomingConnectionsThread=new IncomingConnectionsThread(this);
        new Thread(this.incomingConnectionsThread).start();
    }

    public void setIncomingConnectionsThread(IncomingConnectionsThread thread){
        this.incomingConnectionsThread = thread;
        new Thread(this.incomingConnectionsThread).start();
    }

    public LocalDataSystem getLocalDataSystem() {
        return LocalDataSystem;
    }

    public ServerNetworkInfo getServerNetworkInfo() {
        return this.serverNetworkInfo;
    }

    public IncomingConnectionsThread getIncomingConnectionsThread() {
        return incomingConnectionsThread;
    }

    public DataDistributionManager getDataDistributionManager() {
        return distributionManager;
    }

    public ServerDistributionManager getServerDistributionManager() {
        return distributionManager;
    }

    public void setDistributionManager(DistributionManager DistributionManager) {
        this.distributionManager = DistributionManager;
    }

    public DistributionManager getDistributionManager() {
        return distributionManager;
    }

    public ServerSegmentsStruct getServerRanges() {
        ServerSegmentsStruct returnStructure=new ServerSegmentsStruct();
        returnStructure.serverNetworkInfo=this.serverNetworkInfo;
        returnStructure.tagRanges=this.getLocalDataSystem().getTagRanges();
        returnStructure.dataRanges=this.getLocalDataSystem().getDataRanges();
        return returnStructure;
    }

    public void setServerNetworkInfo(ServerNetworkInfo serverNetworkInfo) {
        this.serverNetworkInfo = serverNetworkInfo;
    }
}
