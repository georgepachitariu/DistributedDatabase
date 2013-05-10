package ConsistentHashing.HelpingClasses;

import ConsistentHashing.HashRange;
import networkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/16/13
 * Time: 5:41 PM
 */
public class ServerSegmentsStruct {
    public LinkedList<HashRange> dataRanges;
    public LinkedList<HashRange> tagRanges;
    public ServerNetworkInfo serverNetworkInfo;

    public ServerSegmentsStruct() {
        this.dataRanges = new LinkedList<HashRange>();
        this.tagRanges = new LinkedList<HashRange>();
    }

    public ServerSegmentsStruct(LinkedList<HashRange> dataRanges,
                                LinkedList<HashRange> tagRanges,
                                ServerNetworkInfo serverNetworkInfo) {
        this.dataRanges = dataRanges;
        this.tagRanges = tagRanges;
        this.serverNetworkInfo = serverNetworkInfo;
    }
}
