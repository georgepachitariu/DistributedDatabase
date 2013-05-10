package Data;

import ConsistentHashing.DataDistributionManager;
import ConsistentHashing.DistributionManager;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import ConsistentHashing.ServerDistributionManager;
import networkInfrastructure.IncomingConnectionsThread;
import networkInfrastructure.ServerNetworkInfo;

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

    public LocalDataSystem getLocalDataSystem() {
        return LocalDataSystem;
    }

    public DatabaseSystem() {
        this.incomingConnectionsThread=new IncomingConnectionsThread(this);
        this.distributionManager=new DistributionManager(this);
        this.LocalDataSystem=new LocalDataSystem();
    }

    public DatabaseSystem(ServerNetworkInfo serverNetworkInfo,
                          Data.LocalDataSystem localDataSystem,
                          DistributionManager distributionManager) {
        this.serverNetworkInfo = serverNetworkInfo;
        LocalDataSystem = localDataSystem;
        this.distributionManager = distributionManager;
     }

    public DatabaseSystem(ServerNetworkInfo serverNetworkInfo) {
        this.serverNetworkInfo = serverNetworkInfo;
    }

    public void setIncomingConnectionsThread(IncomingConnectionsThread thread){
        this.incomingConnectionsThread = thread;
        new Thread(this.incomingConnectionsThread).start();
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

    public ServerSegmentsStruct getServerRanges() {
        ServerSegmentsStruct returnStructure=new ServerSegmentsStruct();
        returnStructure.serverNetworkInfo=this.serverNetworkInfo;
        returnStructure.tagRanges=this.getLocalDataSystem().getTagRanges();
        returnStructure.dataRanges=this.getLocalDataSystem().getDataRanges();
        return returnStructure;
    }
}
