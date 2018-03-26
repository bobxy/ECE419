package app_kvECS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Collection;

import org.apache.zookeeper.KeeperException;

import ecs.IECSNode;

public interface IECSClient {
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start_ecs() throws Exception;

    /**
     * Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean stop_ecs() throws Exception;

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception;

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws NoSuchAlgorithmException 
     * @throws UnsupportedEncodingException 
     * @throws IOException 
     * @throws Exception 
     */
    public IECSNode addNode(String cacheStrategy, int cacheSize) throws UnsupportedEncodingException, NoSuchAlgorithmException, KeeperException, InterruptedException, IOException, Exception;

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     * @return  set of strings containing the names of the nodes
     * @throws NoSuchAlgorithmException 
     * @throws UnsupportedEncodingException 
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws IOException 
     * @throws Exception 
     */
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws UnsupportedEncodingException, NoSuchAlgorithmException, KeeperException, InterruptedException, IOException, Exception;

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * @return  array of strings, containing unique names of servers
     * @throws NoSuchAlgorithmException 
     * @throws UnsupportedEncodingException 
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws Exception 
     */
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) throws UnsupportedEncodingException, NoSuchAlgorithmException, KeeperException, InterruptedException, Exception;

    /**
     * Wait for all nodes to report status or until timeout expires
     * @param count     number of nodes to wait for
     * @param timeout   the timeout in milliseconds
     * @return  true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) throws Exception;

    /**
     * Removes nodes with names matching the nodeNames array
     * @param nodeNames names of nodes to remove
     * @return  true on success, false otherwise
     * @throws Exception 
     */
    public boolean removeNodes(Collection<String> nodeNames) throws Exception;

    /**
     * Get a map of all nodes
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public Map<String, IECSNode> getNodes() throws KeeperException, InterruptedException;

    /**
     * Get the specific node responsible for the given key
     * @throws NoSuchAlgorithmException 
     * @throws UnsupportedEncodingException 
     */
    public IECSNode getNodeByKey(String Key) throws UnsupportedEncodingException, NoSuchAlgorithmException;
}
