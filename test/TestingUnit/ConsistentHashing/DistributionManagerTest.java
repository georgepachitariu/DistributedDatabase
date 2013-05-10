
package TestingUnit.ConsistentHashing;

import ConsistentHashing.DistributionManager;
import ConsistentHashing.HashRange;
import ConsistentHashing.HelpingClasses.DataMovingCoordinatesStruct;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import junit.framework.Assert;
import networkInfrastructure.ServerNetworkInfo;

import java.util.LinkedList;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/16/13
 * Time: 11:51 AM
 */



public class DistributionManagerTest {

    @org.junit.Test
    public void testGetNewVirtualNodeWithMetadata_ForTheFirstPoint() {
        DistributionManager test= new DistributionManager(100,0,new Random(),null, null);

       DataMovingCoordinatesStruct el = test.createNewVirtualNodeWithMetadata(null);

        Assert.assertEquals(el.existingNodeListID, null);
        Assert.assertEquals(el.existingNodeSegment, null);
        Assert.assertEquals(el.newNodeSegment.startPoint, 0);
        Assert.assertEquals(el.newNodeSegment.endPoint, 100);
    }

    @org.junit.Test
    public void testGetNewVirtualNodeWithMetadata_ForASingleServerAndASingleSegment() {

        LinkedList<HashRange> pairList=
                new LinkedList<HashRange>();
        pairList.add(new HashRange(0,100));

        LinkedList<ServerSegmentsStruct> testList=
                new LinkedList<ServerSegmentsStruct>();

        testList.add(new ServerSegmentsStruct(pairList,null,new ServerNetworkInfo("hello:1")));

        DistributionManager test= new DistributionManager(100,0,new Random(),null,null);

        DataMovingCoordinatesStruct el = test.createNewVirtualNodeWithMetadata(testList);

        Assert.assertEquals(el.existingNodeListID.size(), 1);
        Assert.assertEquals(true,el.existingNodeListID.getFirst().toString().equals(new ServerNetworkInfo("hello:1").toString()));

        Assert.assertEquals(el.existingNodeSegment.startPoint, 0);
        Assert.assertEquals(el.existingNodeSegment.endPoint, 100);
        Assert.assertEquals(el.newNodeSegment.endPoint, 100);

        // tests that the point generated belongs to the hash range
        Assert.assertEquals(el.newNodeSegment.startPoint <100
                && el.newNodeSegment.startPoint >=0, true);
    }


    @org.junit.Test
    public void testGetNewVirtualNodeWithDataMovingCoordinatesStruct_ForMoreServersAndMoreMatchingSegments() {

        LinkedList<HashRange> pairList1=
                new LinkedList<HashRange>();
        pairList1.add(new HashRange(0,25));
        pairList1.add(new HashRange(50,72));
        pairList1.add(new HashRange(72,100));

        LinkedList<HashRange> pairList2=
                new LinkedList<HashRange>();
        pairList2.add(new HashRange(25,50));
        pairList2.add(new HashRange(50,72));

        LinkedList<ServerSegmentsStruct> testList=
                new LinkedList<ServerSegmentsStruct>();

        // mocking the random object so that it always return 56
        Random rand=mock(Random.class);
        when(rand.nextLong()).thenReturn((long)56);

        DistributionManager test= new DistributionManager(100,0,rand,null, null);

        testList.add(new ServerSegmentsStruct(pairList1,null, new ServerNetworkInfo("hello:1")));
        testList.add(new ServerSegmentsStruct(pairList2,null, new ServerNetworkInfo("hello:2")));


        DataMovingCoordinatesStruct el = test.createNewVirtualNodeWithMetadata(testList);

        Assert.assertEquals(el.existingNodeListID.size(), 2);
        Assert.assertEquals(true, el.existingNodeListID.get(0).toString().equals(new ServerNetworkInfo("hello:1").toString()));
        Assert.assertEquals(true, el.existingNodeListID.get(1).toString().equals(new ServerNetworkInfo("hello:2").toString()));

        Assert.assertEquals(el.existingNodeSegment.startPoint, 50);
        Assert.assertEquals(el.existingNodeSegment.endPoint, 72);
        Assert.assertEquals(el.newNodeSegment.startPoint, 56);
        Assert.assertEquals(el.newNodeSegment.endPoint, 72);
    }


}

