package TestingUnit.ConsistentHashing;

import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import junit.framework.Assert;
import NetworkInfrastructure.ServerNetworkInfo;
import org.junit.Test;

import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/18/13
 * Time: 11:48 PM
 */
public class ServerSegmentsStructTest {
    @Test
    public void testingPair_toString_and_constructor() {
        LinkedList<HashRange> dataRanges= new LinkedList<HashRange>();
        dataRanges.add(new HashRange(0,100));
        dataRanges.add(new HashRange(200,300));
        dataRanges.add(new HashRange(300,400));

        LinkedList<HashRange> tagRanges= new LinkedList<HashRange>();
        tagRanges.add(new HashRange(50,100));
        tagRanges.add(new HashRange(250,350));
        tagRanges.add(new HashRange(350,450));

        ServerNetworkInfo serverNetworkInfo=
                new ServerNetworkInfo("127.0.0.1:20300");
        ServerSegmentsStruct a =new ServerSegmentsStruct(
                dataRanges,tagRanges,serverNetworkInfo);

        ServerSegmentsStruct b = new ServerSegmentsStruct(a.toString());
        Assert.assertTrue(a.toString().equals(b.toString()));
    }
}
