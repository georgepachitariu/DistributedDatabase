package TestingUnit.ConsistentHashing;

import ConsistentHashing.HelpingClasses.HashRange;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/29/13
 * Time: 4:56 PM
 */
public class HashRangeTest {

    @Test
    public void testingPair_toString_and_constructor() {
        HashRange a =new HashRange(133,2567);
        HashRange b = new HashRange(a.toString());
        Assert.assertEquals(a.startPoint,b.startPoint);
        Assert.assertEquals(a.endPoint,b.endPoint);
    }
}
