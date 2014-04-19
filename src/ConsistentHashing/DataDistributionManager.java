package ConsistentHashing;

import NetworkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/5/13
 * Time: 10:36 AM
 */
public interface DataDistributionManager {
    public LinkedList<ServerNetworkInfo> getServersResponsibleForImageHash(long imageHash);
    public LinkedList<ServerNetworkInfo> getServersResponsibleForTagHash(long tagHash);

}
