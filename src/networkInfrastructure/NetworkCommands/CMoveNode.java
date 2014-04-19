package NetworkInfrastructure.NetworkCommands;

import ConsistentHashing.HelpingClasses.HashRange;
import Data.DatabaseSystem;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/26/13
 * Time: 7:01 PM
 */
public class CMoveNode extends NetworkCommand {
    HashRange range;
    boolean isDataNode;

    public CMoveNode(ServerNetworkInfo server, HashRange range, boolean isDataNode) {
        super(server);
        this.range=range;
        this.isDataNode=isDataNode;
    }

    @Override
    public String getCode() {
        return "CMoveNode";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        // we send the type (if is DataNode or not [tag Node]) and the range
        out.writeBoolean(isDataNode);
        out.writeUTF(range.toString());
        out.flush();

        boolean respone=in.readBoolean();
        super.requestClose();
        return respone;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        boolean isDataNode = in.readBoolean();
        HashRange range= new HashRange(in.readUTF());

        databaseSystem.getDistributionManager().
                attachNewVirtualNodeWithoutDuplication(range, isDataNode);

        // we update the other servers about the new Node added
        databaseSystem.getDistributionManager().updateRangesOnAllServers();

        out.writeBoolean(true);
        out.flush();
    }

    }
