package networkInfrastructure.NetworkCommands;

import ConsistentHashing.HashRange;
import Data.DatabaseSystem;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/28/13
 * Time: 9:58 PM
 */
public class CRequestRawTags extends NetworkCommand {
    public CRequestRawTags(HashRange dataRange, ServerNetworkInfo source,
                     DatabaseSystem destinationServer) {
        super(source);
    }

    @Override
    public String getCode() {
        return "CRequestRawTags";  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean request() throws IOException {
     return false;
      }

    @Override
    public void respond(DataOutputStream out, DataInputStream in, DatabaseSystem databaseSystem) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }


}
