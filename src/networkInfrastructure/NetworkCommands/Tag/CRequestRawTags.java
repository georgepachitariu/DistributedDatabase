package NetworkInfrastructure.NetworkCommands.Tag;

import ConsistentHashing.HelpingClasses.HashRange;
import Data.DatabaseSystem;
import NetworkInfrastructure.NetworkCommands.NetworkCommand;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/28/13
 * Time: 9:58 PM
 */
public class CRequestRawTags extends NetworkCommand {

    private HashRange tagRange;
    private DatabaseSystem requester;

    public CRequestRawTags(HashRange tagRange, ServerNetworkInfo source,
                           DatabaseSystem requestServer) {
        super(source);
        this.tagRange=tagRange;
        this.requester=requestServer;
    }

    @Override
    public String getCode() {
        return "CRequestRawTags";
    }

    public boolean request() throws IOException {
        super.request();
        //we add the hashRange for which we want the raw data
        out.writeUTF(tagRange.toString());
        out.flush();

        int nr=in.readInt();
        for(int i=0; i<nr; i++) {
            long tagKey = in.readLong();

            int k=in.readInt();
            for(int j=0; j<k; j++) {
                long tagValue = in.readLong();

                boolean succeeded=requester.getLocalDataSystem().
                        insertTag(tagKey, tagValue);
                if(succeeded == false)
                    return false;
            }
            out.writeBoolean(true);  //acceptance byte
            out.flush();
        }

        super.requestClose();
        return true;
     }

    @Override
    public void respond(DataOutputStream out, DataInputStream in, DatabaseSystem databaseSystem) throws IOException {
        HashRange range=new HashRange(in.readUTF());
        LinkedList<Long> tagKeysList =
                databaseSystem.getLocalDataSystem().getAllTagsInRange(range);

        //we send the number of tagKeys
        out.writeInt(tagKeysList.size());
        out.flush();

        for(Long key : tagKeysList) {
            out.writeLong(key);

            LinkedList<Long> tagsAssociated =
                    databaseSystem.getLocalDataSystem().getImageHashes(key);
            out.writeInt(tagsAssociated.size());
            for(Long tag: tagsAssociated)
                out.writeLong(tag);

            out.flush();
            // we read the acceptance byte
            if(! in.readBoolean())
                break;
        }
    }


}
