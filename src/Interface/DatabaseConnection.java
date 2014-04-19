package Interface;

import ConsistentHashing.HelpingClasses.HashingFunction;
import Data.ImageWithMetadata;
import NetworkInfrastructure.NetworkCommands.Image.CDeleteImage;
import NetworkInfrastructure.NetworkCommands.Image.CGetImage;
import NetworkInfrastructure.NetworkCommands.Image.CInsertImage;
import NetworkInfrastructure.NetworkCommands.Tag.CDeleteTag;
import NetworkInfrastructure.NetworkCommands.Tag.CGetImageHashCollectionByTag;
import NetworkInfrastructure.NetworkCommands.Tag.CInsertTag;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 5:12 PM
 */
public class DatabaseConnection {
    private ClientTools tools;
    private LinkedList<ServerNetworkInfo> allServers;
    private HashingFunction hashingFunction;
    private Random rand;

    public DatabaseConnection(LinkedList<ServerNetworkInfo> allServers) {
        this(allServers, new HashingFunction(), new ClientTools(), new Random());
    }

    public DatabaseConnection(LinkedList<ServerNetworkInfo> allServers,
                              HashingFunction hashingFunction, ClientTools tools, Random rand) {
        this.allServers = allServers;
        this.hashingFunction=hashingFunction;
        this.tools=tools;
        this.rand=rand;
    }

    public boolean insert(ImageWithMetadata img) {

        // 1. we get the server responsible for the ImageHash
        long imageHash = hashingFunction.getImageHash(img);
        LinkedList<ServerNetworkInfo> imageServers = tools.getServersResponsibleForImageHash(
                allServers.getFirst(), imageHash);

        // 2. we insert the image on all the servers
        for(ServerNetworkInfo imageServer : imageServers) {
            try {
                // 3. we tell him to store the imageWithMetadata
                CInsertImage command=new CInsertImage(imageServer,img);
                boolean response=command.request();
                if(! response) return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        // 4. if the response was true, then for each tag:
        // we insert it on all the replicas that ar responsible for it
        for(String tag : img.tags) {
            // 4.1 we get the server responsible for the tag
            long tagHash = hashingFunction.getTagHash(tag);

            LinkedList<ServerNetworkInfo>  tagServers = tools.getServersResponsibleForTag(
                    allServers.getFirst(), tagHash);
            for(ServerNetworkInfo tagServer : tagServers) {
                try {
                    // 4.3 we insert in that server the tag
                    CInsertTag command=new CInsertTag(tagServer,tag, imageHash);
                    boolean response=command.request();
                    if(! response) return false;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }

    public LinkedList<ImageWithMetadata> get(LinkedList<String> tags) {
        try {
            // For each tag we collect the imageHashes for it.
            LinkedList<Long> allHashesList=new LinkedList<Long>();
            for(String tag : tags) {
                long tagHash=this.hashingFunction.getTagHash(tag);

                // 1. We get the servers Responsible for Tag
                LinkedList<ServerNetworkInfo> serversRequested = this.tools.
                        getServersResponsibleForTag(allServers.getFirst(), tagHash);

                // 2. Get the collection of hashes of images that have the searched Tag
                // from one of the servers responsible for it (selected randomly)
                int j=this.rand.nextInt(serversRequested.size());
                CGetImageHashCollectionByTag command=
                        new CGetImageHashCollectionByTag(serversRequested.get(j), tag);
                command.request();

                // here we cumulate all the hashes
                allHashesList.addAll(command.getImagesHashList());
            }

            // Here we filter the images that have all the tags that we searched for
            LinkedList<Long> filteredList= new FilterList().filterList(allHashesList,tags.size());

            LinkedList<ImageWithMetadata>imageList=
                    new LinkedList<ImageWithMetadata>();

            // 3. For each element in collection we get the image
            for(Long el : filteredList) {
                LinkedList<ServerNetworkInfo> serv = tools.
                        getServersResponsibleForImageHash(allServers.getFirst(), el);
                // from one of the servers responsible for it (selected randomly)
                int j=this.rand.nextInt(serv.size());
                CGetImage comm=new CGetImage(serv.get(j), el);
                comm.request();
                imageList.add(comm.getImageWithMetadata());
            }
            return imageList;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean delete( ImageWithMetadata img ) {

        // 1. we get the server responsible for the ImageHash
        long imageHash = hashingFunction.getImageHash(img);
        LinkedList<ServerNetworkInfo> imageServers = tools.getServersResponsibleForImageHash(
                allServers.getFirst(), imageHash);

        // 2. we delete the image from all of the above servers
        LinkedList<String> tags=null;
        for(ServerNetworkInfo imageServer : imageServers) {
            try {
                // 3. we tell him to delete the imageWithMetadata and we get the tags
                CDeleteImage command=new CDeleteImage(imageServer,imageHash);
                boolean response=command.request();
                if(! response) return false;
                if(tags ==null)
                    tags= command.getTags();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        // 4. we delete each tag from all the servers
        for(String tag : tags) {
            // 4.1 we get the servers responsible for the tag
            long tagHash = hashingFunction.getTagHash(tag);
            LinkedList<ServerNetworkInfo> tagServers = tools.getServersResponsibleForTag(
                    allServers.getFirst(), tagHash);
            // 4.2. we delete the tag from all of the above servers
            for(ServerNetworkInfo tagServer : tagServers) {
                try {
                    // 4.3 we delete the tag from that server
                    CDeleteTag command=new CDeleteTag(tagServer, tagHash, imageHash);
                    boolean response=command.request();
                    if(response==false) return false;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;         // 5. if all the deletions were good return true.
    }
}
