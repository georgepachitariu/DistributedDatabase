package Interface;

import ConsistentHashing.HashingFunction;
import Data.ImageWithMetadata;
import networkInfrastructure.NetworkCommands.Image.CGetImage;
import networkInfrastructure.NetworkCommands.Image.CInsertImage;
import networkInfrastructure.NetworkCommands.Tag.CGetImageHashCollectionByTag;
import networkInfrastructure.NetworkCommands.Tag.CInsertTag;
import networkInfrastructure.ServerNetworkInfo;

import java.io.IOException;
import java.util.LinkedList;

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

    public DatabaseConnection(LinkedList<ServerNetworkInfo> allServers) {
        this(allServers, new HashingFunction(), new ClientTools());
    }

    public DatabaseConnection(LinkedList<ServerNetworkInfo> allServers,
                              HashingFunction hashingFunction, ClientTools tools) {
        this.allServers = allServers;
        this.hashingFunction=hashingFunction;
        this.tools=tools;
    }

    public boolean insert(ImageWithMetadata img) {

        // 1. we get the server responsible for the ImageHash
        long imageHash = hashingFunction.getImageHash(img);
        ServerNetworkInfo imageServer = tools.getServerResponsibleForImageHash(
                allServers.getFirst(), imageHash);

        // 2. we get the most free server from the above list
        //-------------------------------------

        boolean response;
        try {
            // 3. we tell him to store the imageWithMetadata
            CInsertImage command=new CInsertImage(imageServer,img);
            response=command.request();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        // 4. if the response was true, then for each tag:
        if(response) {
            for(String tag : img.tags) {
                // 4.1 we get the server responsible for the tag
                long tagHash = hashingFunction.getTagHash(tag);
                ServerNetworkInfo tagServer = tools.getServerResponsibleForTag(
                        allServers.getFirst(), tagHash);
                // 4.2 we get the most free server from the list
                //------------------------------------

                try {
                    // 4.3 we insert in that server the tag
                    CInsertTag command=new CInsertTag(tagServer,tag, imageHash);
                    response=command.request();
                    if(response==false) return false;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;         // 5. if all the insertions were good return true.
        }

        return false;
    }

    public LinkedList<ImageWithMetadata> get(String tag) {
        try {
            long tagHash=this.hashingFunction.getTagHash(tag);

            // 1. We get the server Responsible for Tag
            ServerNetworkInfo serverRequested = this.tools.
                    getServerResponsibleForTag(allServers.getFirst(), tagHash);

            // 2. Get the collection of hashes of images that have the searched Tag
            CGetImageHashCollectionByTag command=
                    new CGetImageHashCollectionByTag(serverRequested, tag);
            command.request();

            LinkedList<Long> list = command.getImagesHashList();
            LinkedList<ImageWithMetadata>imageList=
                    new LinkedList<ImageWithMetadata>();

            // 3. For each element in collection we get the image
            for(Long el : list) {
                ServerNetworkInfo serv = tools.
                        getServerResponsibleForImageHash(allServers.getFirst(), el);
                CGetImage comm=new CGetImage(serv, el);
                comm.request();
                imageList.add(comm.getImageWithMetadata());
            }
            return imageList;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
