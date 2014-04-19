package ConsistentHashing;

import NetworkInfrastructure.ServerNetworkInfo;

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
    public void attachNewDataVirtualNode( );
    public void attachNewTagVirtualNode( );
    public LinkedList<ServerNetworkInfo> getServersAddresses();

}
