package ConsistentHashing.HelpingClasses;

import ConsistentHashing.HashRange;
import networkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/16/13
 * Time: 3:24 PM
 */
public class DataMovingCoordinatesStruct {
    public LinkedList<ServerNetworkInfo> existingNodeListID;

    public HashRange existingNodeSegment;
    public HashRange newNodeSegment;

    public DataMovingCoordinatesStruct( LinkedList<ServerNetworkInfo> existingNodeListID,
                                        HashRange existingNodeSegment, HashRange newNodeSegment) {
        this.existingNodeListID = existingNodeListID;
        this.existingNodeSegment = existingNodeSegment;
        this.newNodeSegment = newNodeSegment;
    }

    public DataMovingCoordinatesStruct() {
        this.existingNodeListID=new LinkedList<ServerNetworkInfo>();
    }
}
