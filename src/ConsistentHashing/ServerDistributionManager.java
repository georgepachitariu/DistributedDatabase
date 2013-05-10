package ConsistentHashing;

import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import networkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/5/13
 * Time: 10:39 AM
 */
public interface ServerDistributionManager {
    public long getMaxImageHashValue() ;
    public long getMaxTagHashValue();
    public HashRange getNewDataVirtualNode( ServerSegmentsStruct currentServer,
                                            LinkedList<ServerSegmentsStruct> existingServers  );
    public HashRange getNewTagVirtualNode( ServerSegmentsStruct currentServer,
                                           LinkedList<ServerSegmentsStruct> existingServers  );
    public LinkedList<ServerNetworkInfo> getServersAddresses();

}
