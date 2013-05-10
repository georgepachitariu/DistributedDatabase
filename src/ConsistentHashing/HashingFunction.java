package ConsistentHashing;

import Data.ImageWithMetadata;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/22/13
 * Time: 1:09 PM
 */
public class HashingFunction {

    public long getImageHash(ImageWithMetadata image)  {
        long maxRange = new DistributionManager().getMaxImageHashValue();
        return Math.abs(image.raw.hashCode()) %maxRange;
     }

     public long getTagHash(String tag)  {
         long maxTagRange = new DistributionManager().getMaxTagHashValue();
         return Math.abs(tag.hashCode()) %maxTagRange;
     }
}
