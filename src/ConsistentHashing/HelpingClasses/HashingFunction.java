package ConsistentHashing.HelpingClasses;

import ConsistentHashing.DistributionManager;
import Data.ImageWithMetadata;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/22/13
 * Time: 1:09 PM
 */
public class HashingFunction {

    public long getImageHash(ImageWithMetadata image)  {
        long maxRange = new DistributionManager(1,1).getMaxImageHashValue();
        return Math.abs(image.hashCode()) %maxRange;
     }

     public long getTagHash(String tag)  {
         long maxTagRange = new DistributionManager(1,1).getMaxTagHashValue();
         return Math.abs(tag.hashCode()) %maxTagRange;
     }
}
