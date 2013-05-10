package Interface;

import networkInfrastructure.NetworkCommands.CGetServersAddresses;
import networkInfrastructure.ServerNetworkInfo;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 5:09 PM
 */
public class Database {
    public DatabaseConnection connectTo(ServerNetworkInfo server) throws IOException {
        CGetServersAddresses command=new CGetServersAddresses(server);
        command.request();
        return new DatabaseConnection( command.getServersRequested() );
    }
}
