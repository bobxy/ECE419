package common;

import ecs.IECSNode;

public interface KVMessage {

	public enum StatusType {
		GET, 			/* Get - request 0*/
		GET_ERROR, 		/* requested tuple (i.e. value) not found 1*/
		GET_SUCCESS, 	/* requested tuple (i.e. value) found 2*/
		PUT, 			/* Put - request 3*/
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted 4*/
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated 5*/
		PUT_ERROR, 		/* Put - request not successful 6*/
		DELETE_SUCCESS, /* Delete - request successful 7*/
		DELETE_ERROR, 	/* Delete - request successful 8*/

		SERVER_STOPPED,         /* Server is stopped, no requests are processed 9*/
		SERVER_WRITE_LOCK,      /* Server locked for out, only get possible 10*/
		SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key 11*/
		
		PUT_WITHOUT_CACHING,
	}

	/**
	 * @return the key that is associated with this message,
	 * 		null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 * 		null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
     * @return  the responsible server node
     */
    public IECSNode getResponsibleServer();

}
