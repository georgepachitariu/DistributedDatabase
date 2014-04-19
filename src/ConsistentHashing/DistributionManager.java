package ConsistentHashing;

import ConsistentHashing.HelpingClasses.DataMovingCoordinatesStruct;
import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import Data.LocalDataSystem;
import NetworkInfrastructure.NetworkCommands.*;
import NetworkInfrastructure.NetworkCommands.Image.CRequestRawData;
import NetworkInfrastructure.NetworkCommands.Tag.CRequestRawTags;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/16/13
 * Time: 10:46 AM
 */
public class DistributionManager implements DataDistributionManager,
        ServerDistributionManager {

    private long MaxImageHashValue;
    private long MaxTagHashValue;
    private  int imageDuplicationNr;
    private  int tagDuplicationNr;
    private Random rand;
    private DatabaseSystem databaseSystem;
    private LinkedList<ServerSegmentsStruct> existingServers;

    public DistributionManager(int imageDuplicationNr, int tagDuplicationNr) {
        this(  //Long.MAX_VALUE, Long.MAX_VALUE,
                100,100,
                new Random(), new LinkedList<ServerSegmentsStruct>(),
                imageDuplicationNr,  tagDuplicationNr);
    }

    public DistributionManager(int imageDuplicationNr, int tagDuplicationNr,
                               LinkedList<ServerSegmentsStruct> existingServers) {
        this(  //Long.MAX_VALUE, Long.MAX_VALUE,
                100, 100, new Random(), existingServers,
                imageDuplicationNr,  tagDuplicationNr);
    }

    public DistributionManager( Random rand,
                                int imageDuplicationNr, int tagDuplicationNr) {
        this( //Long.MAX_VALUE, Long.MAX_VALUE,
                100,100,
                rand ,
                new LinkedList<ServerSegmentsStruct>(),
                imageDuplicationNr,  tagDuplicationNr);
    }

    public DistributionManager(  long maxImageHashValue,
                                 long maxTagHashValue, Random rand,
                                 LinkedList<ServerSegmentsStruct> existingServers,
                                 int imageDuplicationNr, int tagDuplicationNr
    ) {
        this.MaxImageHashValue = maxImageHashValue;
        this.MaxTagHashValue=maxTagHashValue;
        this.rand = rand;
        this.imageDuplicationNr=imageDuplicationNr;
        this.tagDuplicationNr=tagDuplicationNr;
        this.existingServers=existingServers;
    }

    public void setDatabaseSystem(DatabaseSystem databaseSystem) {
        this.databaseSystem = databaseSystem;

        // we add the current server in the existingServers list
        this.existingServers.add( new ServerSegmentsStruct(
                this.databaseSystem.getServerNetworkInfo())
        );
    }

    public void setExistingServers(LinkedList<ServerSegmentsStruct> existingServers) {
        this.existingServers = existingServers;
    }

    @Override
    public long getMaxImageHashValue() {
        return MaxImageHashValue;
    }

    @Override
    public long getMaxTagHashValue() {
        return MaxTagHashValue;
    }

    @Override
    /**
     * It Attaches a new virtual node and sends the data from the
     *  other servers to it
     */
    public void attachNewDataVirtualNode( ) {
        int size=this.existingServers.size();

        HashRange h=null;
        if(size<=this.imageDuplicationNr && size>=2 )
            // it means that we just copy the associated hashRange from the prevous server
            // (for the first range in this server we copy the first range from the previous server,
            // for the second range, we copy the second range  from the prevoius server,
            // and so on)
            h =this.existingServers.getFirst().dataRanges.get(
                    this.databaseSystem.getLocalDataSystem().getDataRanges().size()
            );

        HashRange hashRange = this.attachNewVirtualNode(true, h);

        if(size<=this.imageDuplicationNr && size>=2 )
            // if we just copy the previous node we don't have to insert
            // the new hash range to the previous nodes
            this.existingServers.getLast().dataRanges.add(hashRange);
        else {
            // we add the hashRange to this and the last (imageDuplicationNr-1)
            // servers to assure duplication
            int i=this.existingServers.size()-1;
            int k=this.imageDuplicationNr;
            for(;  i>=0 && k>0;   i--, k--)
                this.existingServers.get(i).dataRanges.add(hashRange);
        }
    }

    @Override
    /**
     * It attaches a new virtual node and sends the data from the
     *  other servers to it
     */
    public void attachNewTagVirtualNode( ) {
        int size=this.existingServers.size();

        HashRange h=null;
        if(size<=this.tagDuplicationNr && size>=2 )
            // it means that we just copy the associated hashRange from the prevous server
            // (for the first range in this server we copy the first range from the previous server,
            // for the second range, we copy the second range  from the prevoius server,
            // and so on)
            h =this.existingServers.getFirst().tagRanges.get(
                    this.databaseSystem.getLocalDataSystem().getTagRanges().size()
            );

        HashRange hashRange = this.attachNewVirtualNode(false, h);

        if(size<=this.tagDuplicationNr && size>=2 )
            // if we just copy the previous node we don't have to insert
            // the new has range to the previous nodes
            this.existingServers.getLast().tagRanges.add(hashRange);
        else {
            // we add the hashRange to this and the last (imageDuplicationNr-1)
            // servers to assure duplication
            int i=this.existingServers.size()-1;
            int k=this.tagDuplicationNr;
            for(;  i>=0 && k>0;   i--, k--)
                this.existingServers.get(i).tagRanges.add(hashRange);
        }
    }

    /**
     * It Attaches a new virtual node and sends the data from the
     *  other servers to it
     */
    public void attachNewVirtualNodeWithoutDuplication(HashRange h, boolean isDataNode) {
        int size=this.existingServers.size();

        HashRange hashRange = this.attachNewVirtualNode(isDataNode, h);

        // we add the hash range to the currently server
        for(ServerSegmentsStruct server: this.existingServers)
            if(server.serverNetworkInfo.equals(this.databaseSystem.getServerNetworkInfo()))
                if(isDataNode)
                    server.dataRanges.add(hashRange);
                else
                    server.tagRanges.add(hashRange);
    }

    /**
     *    creating a new node with h range (or a random one if h==null)
     *
     */
    private HashRange attachNewVirtualNode ( boolean isDataNode, HashRange h  ) {
        LocalDataSystem localDataSystem=this.databaseSystem.getLocalDataSystem();

        //1. creating the new node with the coordinates of the new segment and the old ones
        DataMovingCoordinatesStruct str;
        if(h==null)
            str=this.createNewVirtualNode(existingServers, isDataNode);
        else  // if h isn't null, it's because we are just copying a node
            str=this.createNodeWithPredefinedHashRange(existingServers, h, isDataNode);

        //2. we add the range to the current data system
        if(isDataNode)
            localDataSystem.addDataVirtualNode(str.newNodeSegment);
        else
            localDataSystem.addTagVirtualNode(str.newNodeSegment);

        //3. moving data from the server that was in charge
        // until now for this segment to this server
        if(str.existingNodeListID !=null)
            if( str.existingNodeListID.contains( databaseSystem.getServerNetworkInfo()) ) {
                // a.if the new segment is from the same server
                // we just move the data between directories
                if(isDataNode)
                    localDataSystem.moveDataFromTo(str.existingNodeSegment,
                            str.newNodeSegment);
                else
                    localDataSystem.moveTagsFromTo(str.existingNodeSegment,
                            str.newNodeSegment);

                // we delete the old range
                localDataSystem.resizeOldSegmentToNewOne(
                        str.existingNodeSegment, new HashRange(
                        str.existingNodeSegment.startPoint,
                        str.newNodeSegment.startPoint
                ),
                        isDataNode);
            } else {
                // b. if the new segment is from another server:

                // we select randomly a server from the servers that contains the segment
                int r=rand.nextInt(str.existingNodeListID.size());
                ServerNetworkInfo freeServer=str.existingNodeListID.get(r);

                // creating and sending the command to move data from the servers that contain
                // the old segment this one
                NetworkCommand dataMovingCommand=null;
                if(isDataNode)
                    dataMovingCommand=new CRequestRawData(
                            str.newNodeSegment, freeServer,this.databaseSystem);
                else dataMovingCommand=new CRequestRawTags(
                        str.newNodeSegment, freeServer,this.databaseSystem);
                try {
                    dataMovingCommand.request(); // we make the call
                } catch (IOException e) {
                    System.err.println(dataMovingCommand.getCode()+"- Network command ended unsuccesfully");
                }
            }

        if(h ==null) // only for new nodes (and not the one we copy to assure replicas of the data)
            // we substract from the old hash range the new one (to update the old)
            this.updateOldRange(str.existingNodeSegment, str.newNodeSegment, isDataNode );

        return str.newNodeSegment;
    }

    private void updateOldRange(HashRange existingNodeSegment,
                                HashRange newNodeSegment, boolean isDataNode) {
        if(isDataNode) {
            for(ServerSegmentsStruct server : existingServers)
                if(  server.dataRanges.remove(existingNodeSegment) )
                    // if there was an existing node, we add a new one
                    server.dataRanges.add(new HashRange(
                            existingNodeSegment.startPoint, newNodeSegment.startPoint
                    ));
        }
        else
            for(ServerSegmentsStruct server : existingServers)
                if(  server.tagRanges.remove(existingNodeSegment) )
                    // if there was an existing node, we add a new one
                    server.tagRanges.add(new HashRange(
                            existingNodeSegment.startPoint, newNodeSegment.startPoint
                    ));
    }

    public DataMovingCoordinatesStruct createNewVirtualNode(
            LinkedList<ServerSegmentsStruct> existingServers, boolean isDataNode) {
        long newPoint=getValidRandomNumber(existingServers,isDataNode);

        if(isDataNode) // for the first time calling this method
        {
            if( existingServers.size() == 1 &&
                    existingServers.getFirst().dataRanges.size() ==0 )
                return new DataMovingCoordinatesStruct(null, null,
                        new HashRange(0,this.MaxImageHashValue)  );
        }
        else
        if( existingServers.size() == 1 &&
                existingServers.getFirst().tagRanges.size() ==0 )
            return new DataMovingCoordinatesStruct(null, null,
                    new HashRange(0,this.MaxImageHashValue)  );


        DataMovingCoordinatesStruct returnStructure = new DataMovingCoordinatesStruct();

        for(ServerSegmentsStruct server : existingServers) {
            LinkedList<HashRange> rangeList;
            if(isDataNode)         rangeList=server.dataRanges;
            else                       rangeList=server.tagRanges;
            for(HashRange range : rangeList) {
                if(range.startPoint<=newPoint && range.endPoint>=newPoint) {
                    if(returnStructure.existingNodeListID.size() == 0) {
                        returnStructure.existingNodeSegment=range;
                        returnStructure.newNodeSegment=
                                new HashRange(newPoint, range.endPoint);
                    }
                    returnStructure.existingNodeListID.add(server.serverNetworkInfo);
                }
            }
        }
        return returnStructure;
    }

    private long getValidRandomNumber(
            LinkedList<ServerSegmentsStruct> existingServers, boolean isDataNode) {
        Long newPoint;
        LinkedList<Long > list=this.getAllEndPoints(existingServers,isDataNode);
        boolean passedTest;
        do { // we do not allow for a range to have length 0 (ex start=123, end=123)
            newPoint=Math.abs( this.rand.nextLong())%this.MaxImageHashValue;
            passedTest=true;
            for(Long l : list)
                if(l.longValue() == newPoint.longValue())
                    passedTest=false;
        } while (! passedTest);
        return newPoint;
    }

    private LinkedList<Long> getAllEndPoints(LinkedList<ServerSegmentsStruct> existingServers, boolean isDataNode) {
        LinkedList<Long> returnList=new LinkedList<Long>();
        for(ServerSegmentsStruct server : existingServers) {
            LinkedList<HashRange> rangeList;
            if(isDataNode)         rangeList=server.dataRanges;
            else                       rangeList=server.tagRanges;
            for(HashRange range : rangeList)
                returnList.add(range.endPoint);
        }
        return returnList;
    }

    public DataMovingCoordinatesStruct createNodeWithPredefinedHashRange(
            LinkedList<ServerSegmentsStruct> existingServers, HashRange hashRange,
            boolean isDataNode) {

        DataMovingCoordinatesStruct returnStructure = new DataMovingCoordinatesStruct();
        returnStructure.existingNodeSegment=hashRange;
        returnStructure.newNodeSegment=hashRange;

        for(ServerSegmentsStruct server : existingServers) {
            LinkedList<HashRange> rangeList;
            if(isDataNode)         rangeList=server.dataRanges;
            else                       rangeList=server.tagRanges;
            for(HashRange range : rangeList) {
                if(range.startPoint==hashRange.startPoint &&
                        range.endPoint==hashRange.endPoint) {
                    returnStructure.existingNodeListID.add(server.serverNetworkInfo);
                }
            }
        }
        return returnStructure;
    }

    @Override
    public LinkedList<ServerNetworkInfo> getServersResponsibleForImageHash(long imageHash) {
        LinkedList<ServerNetworkInfo> returnList=new LinkedList<ServerNetworkInfo>();
        for( ServerSegmentsStruct serv : this.existingServers)
            for(HashRange hR : serv.dataRanges)
                if(hR.contains(imageHash))
                    returnList.add(serv.serverNetworkInfo);
        return returnList;
    }

    @Override
    public LinkedList<ServerNetworkInfo> getServersResponsibleForTagHash(long tagHash) {
        LinkedList<ServerNetworkInfo> returnList=new LinkedList<ServerNetworkInfo>();
        for( ServerSegmentsStruct serv : this.existingServers)
            for(HashRange hR : serv.tagRanges)
                if(hR.contains(tagHash))
                    returnList.add(serv.serverNetworkInfo);
        return returnList;
    }

    public LinkedList<ServerNetworkInfo> getServersAddresses() {
        LinkedList<ServerNetworkInfo> returnList=
                new LinkedList<ServerNetworkInfo>();
        for( ServerSegmentsStruct serv : this.existingServers)
            returnList.add(serv.serverNetworkInfo);
        return returnList;
    }

    public int getTagDuplicationNr() {
        return tagDuplicationNr;
    }

    public int getImageDuplicationNr() {
        return imageDuplicationNr;
    }

    public LinkedList<ServerSegmentsStruct> getExistingServers() {
        return existingServers;
    }

    public void updateRangesOnAllServers() {
        try {
            for(ServerSegmentsStruct server : existingServers) {
                if(! server.serverNetworkInfo.equals(databaseSystem.getServerNetworkInfo())) {
                    CUpdateServersRanges cmd=new CUpdateServersRanges (server.serverNetworkInfo, this.existingServers);
                    cmd.request();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reDistributeNodesOf( ServerSegmentsStruct serverDown ) {
        try{
            LinkedList<ServerSegmentsStruct> copy = this.existingServers;
            if(copy.size() > this.imageDuplicationNr) {
                // we only move nodes if nr_of_existing_Servers> nr_of_duplicates
                LinkedList<HashRange> dataRanges = serverDown.dataRanges;
                while(dataRanges.size()>0)
                    for(ServerSegmentsStruct server : copy )
                        if((!server.serverNetworkInfo.equals(serverDown.serverNetworkInfo)) &&
                                // we don't want to move a node to a server that already contains it
                              (!server.dataRanges.contains(dataRanges.peekFirst()))   ) {
                            CMoveNode cmd=new CMoveNode(server.serverNetworkInfo,dataRanges.pop(),true);
                            cmd.request();
                            if(dataRanges.size()==0) break;
                        }
            }

            if(copy.size() > this.tagDuplicationNr) {
                // we only move nodes if nr_of_existing_Servers> nr_of_duplicates
                LinkedList<HashRange> tagRanges = serverDown.tagRanges;
                while(tagRanges.size()>0)
                    for(ServerSegmentsStruct server : copy ) {
                        if((! server.serverNetworkInfo .equals( serverDown.serverNetworkInfo)) &&
                        // we don't want to move a node to a server that already contains it
                                (!server.dataRanges.contains(tagRanges.peekFirst()))   ) {
                            CMoveNode cmd=new CMoveNode(server.serverNetworkInfo,tagRanges.pop(), false);
                            cmd.request();
                            if(tagRanges.size()==0) break;
                        }
                    }
            }

            // we remove serverDown from the "existingServers' and we update the remaining servers of the change
            LinkedList<ServerSegmentsStruct> segList = existingServers;
            for(int i=0;i<segList.size(); i++)
                if(segList.get(i).serverNetworkInfo.equals(serverDown.serverNetworkInfo)) {
                    segList.remove(i);
                    break;
                }
            this.existingServers=segList;
            this.updateRangesOnAllServers();
        }  catch (IOException e) {
            e.printStackTrace();
        }
    }
}

