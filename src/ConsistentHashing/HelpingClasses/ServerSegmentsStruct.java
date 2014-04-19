package ConsistentHashing.HelpingClasses;

import NetworkInfrastructure.ServerNetworkInfo;

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

    public ServerSegmentsStruct(ServerNetworkInfo serverNetworkInfo) {
        this.serverNetworkInfo = serverNetworkInfo;
        this.dataRanges = new LinkedList<HashRange>();
        this.tagRanges = new LinkedList<HashRange>();
    }

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

    public ServerSegmentsStruct(String raw) {
        this();
        String []parts=raw.split("#");

        int i=1;
        for(; i<= Integer.parseInt(parts[0]); i++)
                this.dataRanges.add(new HashRange(parts[i]));

        int k=Integer.parseInt(parts[i]);
        for(int j=0; j<k; j++, i++) {
            this.tagRanges.add(new HashRange(parts[i+1]));
        }

        this.serverNetworkInfo=new ServerNetworkInfo(parts[i+1]);
    }

    @Override
    public String toString() {
        String bulk=new String();
        bulk+=dataRanges.size()+"#";
        for(HashRange h : dataRanges)
            bulk+=h.toString() + "#";

        bulk+=tagRanges.size()+"#";
        for(HashRange h : tagRanges)
            bulk+=h.toString() + "#";

        bulk+=serverNetworkInfo.toString();
        return bulk;
    }

    @Override
    public boolean equals(Object a) {
        if(((ServerSegmentsStruct)a).toString().equals( this.toString()))
            return true;
        return false;
    }
}
