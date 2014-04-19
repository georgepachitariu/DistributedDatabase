package NetworkInfrastructure;

import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import NetworkInfrastructure.NetworkCommands.CPing;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/29/13
 * Time: 1:50 PM
 */
public class PingThread implements Runnable {
    private LinkedList<ServerSegmentsStruct> existingServers;
    private DatabaseSystem databaseSystem;

    public PingThread(LinkedList<ServerSegmentsStruct> existingServers, DatabaseSystem databaseSystem) {
        this.existingServers = existingServers;
        this.databaseSystem = databaseSystem;
    }

    @Override
    public void run() {
        // then we search to see if there are any non-responsive servers
        LinkedList<ServerSegmentsStruct> listCopy = existingServers;
        for(ServerSegmentsStruct server : listCopy)
            if(! server.serverNetworkInfo.equals(databaseSystem.getServerNetworkInfo())) {
                CPing cmd=new CPing(server.serverNetworkInfo);
                try {
                    cmd.request();
                } catch (SocketTimeoutException e) {
                    // then that server is non responsive
                    this.databaseSystem.getDistributionManager().
                            reDistributeNodesOf(server);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }
}
