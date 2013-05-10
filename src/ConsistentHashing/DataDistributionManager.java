package ConsistentHashing;

import networkInfrastructure.ServerNetworkInfo;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/5/13
 * Time: 10:36 AM
 */
public interface DataDistributionManager {
    public ServerNetworkInfo getServersResponsibleForImageHash(long imageHash);
    public ServerNetworkInfo getServersResponsibleForTagHash(long tagHash);

}
