package networkInfrastructure;

import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import networkInfrastructure.NetworkCommands.NetworkCommand;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

public class CUpdateServersRanges extends NetworkCommand {
    LinkedList<ServerSegmentsStruct> newServerRanges;

    public CUpdateServersRanges(ServerNetworkInfo server,
                                LinkedList<ServerSegmentsStruct> newServerRanges) {
        super(server);
        this.newServerRanges=newServerRanges;
    }

    @Override
    public String getCode() {
        return "CUpdateServersRanges";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        // we send the ranges
        out.writeInt(newServerRanges.size());
        for(ServerSegmentsStruct x : newServerRanges) {
            out.writeUTF(x.toString());
            out.flush();
            if(! in.readBoolean())
                return false;
        }

        // finishing bit
        boolean response=in.readBoolean();

        super.requestClose();
        return response;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {

        LinkedList<ServerSegmentsStruct> servers=
                new LinkedList<ServerSegmentsStruct>();
        int nr =in.readInt () ;
        for(int i=0; i<nr; i++) {
            servers.add( new ServerSegmentsStruct( in.readUTF() ));
            out.writeBoolean(true);
            out.flush();
        }

        // we delete the data in the current system
        // that we don't have to store anymore
        for(ServerSegmentsStruct server : servers)
        // here we find the current system
        {
            if (server.serverNetworkInfo.toString().equals(
                    databaseSystem.getServerNetworkInfo().toString()
            )) {
                LinkedList<HashRange> oldRanges =
                        databaseSystem.getLocalDataSystem().getDataRanges();
                LinkedList<HashRange> rangesToBeDeleted =
                        getRangesToBeDeleted(server.dataRanges, oldRanges);
                for (HashRange h : rangesToBeDeleted) // for images (data)
                    databaseSystem.getLocalDataSystem().deleteAllImagesInRange(h);


                oldRanges = databaseSystem.getLocalDataSystem().getTagRanges();
                rangesToBeDeleted = getRangesToBeDeleted(server.tagRanges, oldRanges);
                for (HashRange h : rangesToBeDeleted) // for tags
                    databaseSystem.getLocalDataSystem().deleteAllITagsInRange(h);

                // in the end we update the current segments with the new ones:
                    // a. for the data
                oldRanges = databaseSystem.getLocalDataSystem().getDataRanges();
                updateExistingSegmentsWithNewOnes(databaseSystem, server.dataRanges, oldRanges, true);
                    // b. for the tags
                oldRanges = databaseSystem.getLocalDataSystem().getTagRanges();
                updateExistingSegmentsWithNewOnes(databaseSystem, server.tagRanges, oldRanges, false);


                // we also add the new hashRanges for which is responsible
                // to assure duplication of data
                    // a. for the data
                oldRanges = databaseSystem.getLocalDataSystem().getDataRanges();
                for(HashRange newH : server.dataRanges) {
                    if(! oldRanges.contains(newH))
                        databaseSystem.getLocalDataSystem().addDataVirtualNode(newH);
                }
                    // b. for the tags
                oldRanges = databaseSystem.getLocalDataSystem().getTagRanges();
                for(HashRange newH : server.tagRanges) {
                    if(! oldRanges.contains(newH))
                        databaseSystem.getLocalDataSystem().addTagVirtualNode(newH);
                }
            }
        }

        // in the end we update the old segments with the new ones:
        databaseSystem.getDistributionManager().setExistingServers(servers);

        // we send the finishing bit to the requester
        out.writeBoolean(true);
        out.flush();
    }

    public void updateExistingSegmentsWithNewOnes(
            DatabaseSystem databaseSystem, LinkedList<HashRange> newRanges,
                LinkedList<HashRange> oldRanges, boolean isDataNode) {
        for (HashRange newR : newRanges)
            for(int i=0; i<oldRanges.size(); i++) {
                HashRange oldR=oldRanges.get(i);
                if (oldR.startPoint == newR.startPoint)
                    /*if they are not different they shouldn't be updated*/
                    databaseSystem.getLocalDataSystem().resizeOldSegmentToNewOne(
                            oldR, new HashRange(newR.startPoint, newR.endPoint), isDataNode
                    );
            }
    }

    public LinkedList<HashRange> getRangesToBeDeleted(
            LinkedList<HashRange> newRanges, LinkedList<HashRange> oldRanges) {

        //  We check if the the ranges differ from the old ones
        //  If they do, we collect the differences.
        LinkedList<HashRange> rangesToBeDeleted=
                new LinkedList<HashRange>();

        for(HashRange new_h : newRanges) {
            for(HashRange old_h: oldRanges)
                if(old_h.contains(new_h.startPoint) &&
                        old_h.contains(new_h.endPoint-1) )
                // then the new segment is smaller then the old one
                // and we have to collect the containing data from the
                // left and the right side of the segment
                {
                    // 1.we collect the data
                    // from the left:
                    if(old_h.startPoint != new_h.startPoint)
                        rangesToBeDeleted.add(new HashRange(
                                old_h.startPoint, new_h.startPoint));
                    //from the right:
                    if(old_h.endPoint != new_h.endPoint)
                        rangesToBeDeleted.add(new HashRange(
                                new_h.endPoint, old_h.endPoint));
                }
        }
        return rangesToBeDeleted;
    }
}

