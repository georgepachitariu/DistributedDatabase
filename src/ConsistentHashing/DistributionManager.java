package ConsistentHashing;

import ConsistentHashing.HelpingClasses.DataMovingCoordinatesStruct;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import networkInfrastructure.NetworkCommands.CRequestRawData;
import networkInfrastructure.NetworkCommands.CRequestRawTags;
import networkInfrastructure.NetworkCommands.NetworkCommand;
import networkInfrastructure.ServerNetworkInfo;

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
    private Random rand;
    private DatabaseSystem databaseSystem;
    private LinkedList<ServerSegmentsStruct> existingServers;

    public DistributionManager() {
        this.MaxTagHashValue=Long.MAX_VALUE;
        this.MaxImageHashValue=Long.MAX_VALUE;
    }

    public DistributionManager(DatabaseSystem databaseSystem) {
        this(Long.MAX_VALUE, Long.MAX_VALUE,
                new Random(),  databaseSystem,
                new LinkedList<ServerSegmentsStruct>());
    }

    public DistributionManager(  long maxImageHashValue,
                                 long maxTagHashValue, Random rand,
                                 DatabaseSystem databaseSystem,
                                 LinkedList<ServerSegmentsStruct> existingServers
                                 ) {
        this.MaxImageHashValue = maxImageHashValue;
        this.MaxTagHashValue=maxTagHashValue;
        this.rand = rand;
        this.databaseSystem=databaseSystem;
        this.existingServers=existingServers;
    }

    public void setDatabaseSystem(DatabaseSystem databaseSystem) {
        this.databaseSystem = databaseSystem;
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
    public HashRange getNewDataVirtualNode( ServerSegmentsStruct currentServer,
                                            LinkedList<ServerSegmentsStruct> existingServers  ) {
        return this.getNewVirtualNode(currentServer, existingServers, true);
    }

    @Override
    public HashRange getNewTagVirtualNode( ServerSegmentsStruct currentServer,
                                           LinkedList<ServerSegmentsStruct> existingServers  ) {
        return this.getNewVirtualNode(currentServer, existingServers, false);
    }


    private HashRange getNewVirtualNode ( ServerSegmentsStruct currentServer,
                                          LinkedList<ServerSegmentsStruct> existingServers,  boolean isDataNode
    ) {
        // creating the new node with the coordinates of the new segment and the old ones
        DataMovingCoordinatesStruct returnStructure =
                createNewVirtualNodeWithMetadata(existingServers);

        // getting the most free server
        ServerNetworkInfo freeServer=this.getFreeServerFrom(returnStructure.existingNodeListID);

        // creating and sending the command to move data from the servers that contain
        // the old segment this one
        NetworkCommand dataMovingCommand=null;
        if(isDataNode)
            dataMovingCommand=new CRequestRawData(returnStructure.newNodeSegment, freeServer,
                    this.databaseSystem);
        else dataMovingCommand=new CRequestRawTags(returnStructure.newNodeSegment, freeServer,
                this.databaseSystem);
        try {
        dataMovingCommand.request(); // we make the call
        } catch (IOException e) {
                System.err.println("Network command ended unsuccesfully");
        }
        return returnStructure.newNodeSegment;
    }

    private ServerNetworkInfo getFreeServerFrom(LinkedList<ServerNetworkInfo> existingNodeListID) {
        return null;
    }

    public DataMovingCoordinatesStruct createNewVirtualNodeWithMetadata(
                LinkedList<ServerSegmentsStruct> existingServers) {
        Long newPoint=Math.abs( this.rand.nextLong())%this.MaxImageHashValue;

        if(existingServers == null)
        {
            return new DataMovingCoordinatesStruct(null, null,
                    new HashRange(0,this.MaxImageHashValue)  );
        }

        DataMovingCoordinatesStruct returnStructure = new DataMovingCoordinatesStruct();

        for(ServerSegmentsStruct server : existingServers) {
            for(HashRange range : server.dataRanges) {
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

    @Override
    public ServerNetworkInfo getServersResponsibleForImageHash(long imageHash) {
        for( ServerSegmentsStruct serv : this.existingServers)
            for(HashRange hR : serv.dataRanges)
                if(hR.contains(imageHash))
                    return serv.serverNetworkInfo;

        return null;
    }

    @Override
    public ServerNetworkInfo getServersResponsibleForTagHash(long tagHash) {
        for( ServerSegmentsStruct serv : this.existingServers)
            for(HashRange hR : serv.tagRanges)
                if(hR.contains(tagHash))
                    return serv.serverNetworkInfo;

        return null;
    }

    public LinkedList<ServerNetworkInfo> getServersAddresses() {
        LinkedList<ServerNetworkInfo> returnList=
                new LinkedList<ServerNetworkInfo>();
        for( ServerSegmentsStruct serv : this.existingServers)
            returnList.add(serv.serverNetworkInfo);
        return returnList;
    }
/*
       public ServerSegmentsStruct InsertANewServer( LinkedList<ServerSegmentsStruct> existingServers ) {
        ServerSegmentsStruct set= new ServerSegmentsStruct();

        // we generate an id
        set.ServerID=new Random().nextInt(1000);

        if(existingServers == null)
        {
            // we generate the points
            LinkedList<Long> temp= new LinkedList<Long>();
            for(int i=0; i<3; i++)
                temp.push(Math.abs( new Random().nextLong())%this.MaxHashValue);
            Collections.sort(temp);

            set.Segments.add(new CoordinatesPair(0, temp.getFirst()));
            set.Segments.add(new CoordinatesPair(temp.getLast(), this.MaxHashValue));

            for(int i=0; i<2; i++)
                 set.Segments.add(new CoordinatesPair(temp.get(i), temp.get(i + 1)));

            return set;
        }

        LinkedList<CoordinatesPair> existingSetOfSegments = this.getAllSegmentsFrom(existingServers);

        LinkedList<Long> temp= new LinkedList<Long>();
        // we generate the points
        for(int i=0; i<5; i++)
            temp.push(Math.abs( new Random().nextLong())%this.MaxHashValue);

        for(long newPoint : temp) {

            for(CoordinatesPair pair : existingSetOfSegments) {
                if(pair.StartPoint<=newPoint && pair.EndPoint>=newPoint) {

                }



            }
        }

        return set;
    }
*/
  /*  public LinkedList <DataMovingCoordinatesStruct>
            getDirectionsToMoveDataFromDisconnectedServer(
                ServerPointsStruct BrokenServer,
                LinkedList<ServerPointsStruct> allRunningServers
                        ) {

        LinkedList <DataMovingCoordinatesStruct> returningBlob =
                        new LinkedList <DataMovingCoordinatesStruct>();


            for(long broke_x : BrokenServer.Points) {
                LinkedList<Integer> serversThatContainThisSegment=new LinkedList<Integer>();
                long biggestPointLesserThanCurrentPoint=0; // source ID
                long smallestPointBiggerThanBrokenOne= BrokenServer.ServerID; // destination ID

                for(ServerPointsStruct  server: allRunningServers ) {
                    for(long y : server.Points) {
                        if( y< broke_x )
                            if( y > biggestPointLesserThanCurrentPoint )
                        {
                            serversThatContainThisSegment = new LinkedList<Integer>();
                            serversThatContainThisSegment.add(server.ServerID);

                            biggestPointLesserThanCurrentPoint=y;
                        }
                           else if (y == biggestPointLesserThanCurrentPoint )
                        {
                            serversThatContainThisSegment.add(server.ServerID);
                        }
                    }
                }

                DataMovingCoordinatesStruct temp= new DataMovingCoordinatesStruct();

                temp.SourceID

                returningBlob.add(new DataMovingCoordinatesStruct() {});



            }*/



}

