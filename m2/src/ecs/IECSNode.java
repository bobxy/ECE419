package ecs;

import java.util.Comparator;

public interface IECSNode {

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();
    
    public void setNodeHashRange(String lowerB, String upperB);
    
    public String getNodeHashValue();
    
    public void setNodeHashValue(String value);
    
    public String getNodeStatus();
    
    public void setNodeStatus(String status);
    
}
