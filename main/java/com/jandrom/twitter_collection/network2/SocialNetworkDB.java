package com.jandrom.twitter_collection.network2;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class SocialNetworkDB {

	public static final String SOCIAL = "social";
	public static final String USERS = "users";
	public static final String ID_QUEUE = "id_queue";

	private DBCollection socialNetwork;
	private DBCollection userProfiles;
	private DBCollection idQueue;
	private Map<String, Set<ObjectId>> documentsCheckedOut;

	public SocialNetworkDB() 
		throws UnknownHostException {

		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB("stream_store");
		this.socialNetwork = db.getCollection("social_network");
		this.userProfiles = db.getCollection("user_profiles");
		this.idQueue = db.getCollection("id_queue");
		this.documentsCheckedOut = new HashMap<>();
	}

	/**
	 * TODO: The implementation of this function is changed, the method used before consume too much memories. by Yeqing
	 * Note: A document can not be checkout by Friends and Follower thread at the same time. Function isDocumentCheckOut will
	 * return false in this condition.
	 * Method guarantees that a document in the database is only checked out
	 * once.
	 * 
	 * ie: Thread A checks out document with id 5. Thread B subsequently
	 * requests objects and document id 5 matches the query. The document will
	 * not be returned until thread A checks the document back in.
	 * 
	 * @param query
	 * @return null if there are no more objects in the db matching query. empty
	 *         set if there are more documents but they are checked out.
	 * @throws SocialNetworkException
	 */
	public synchronized Set<DBObject> checkoutDocumentsMatching(
			String requestedBy,
			String collectionName, 
			DBObject query, 
			int count)
			throws SocialNetworkException {

		Set<DBObject> docs = new HashSet<>();
		Set<ObjectId> ids = setFor(requestedBy);
		DBCollection collection = collectionFor(collectionName);

		/* Modify begin by Yeqing,
		only query count number of records.
		Cursor cursor = collection.find(query).limit(
				count * documentsCheckedOut.keySet().size());

		Modify End */
		Cursor cursor = collection.find(query).limit(count);

		if (!cursor.hasNext()) {
            /* Add by Yeqing Yan Begin */
            /* Clean resource to avoid memory leak */
            cursor.close();
            /* Add by Yeqing Yan End */
			return null; // No documents match that query
		}

		int i = 0;
		while (cursor.hasNext() && i < 10) {
			DBObject doc = cursor.next();
			ObjectId id = (ObjectId) doc.get("_id");
			if (!isDocumentCheckedOut(id)) {
				ids.add(id);
				docs.add(doc);
				/* Add by Yeqing Begin at Apr 12
				 * Change documents status to 4 after checkout */
				// Status = 4 means this record is under querying
				if (query.containsField("friends_status")) {
					doc.put("friends_status", 4);
				} else if (query.containsField("following_status")){
					doc.put("following_status", 4);
				} else {
					System.out.println("Meet error, query object didn't contain friends_status or following_status");
				}
				collection.update(new BasicDBObject("_id", id), doc);
				/* Add by Yeqing End*/
				i++;
			}
		}
        /* Add by Yeqing Yan Begin */
        /* Clean resource to avoid memory leak */
        cursor.close();
        /* Add by Yeqing Yan End */
		return docs;
	}

	/**
	 * This will write a document into the database as well
	 * as check in an item that had bee nche
	 * @param requestedBy
	 * @param collectionName
	 * @param id
	 * @param document
	 * @throws SocialNetworkException
	 */
	/* Modified by Yeqing Yan at Apr 14 Begin*/
	/* Remove synchronized keywords here */
	/* Modified by Yeqing Yan End*/
	public void checkinDocument(
			String requestedBy,
			String collectionName,
			ObjectId id, 
			DBObject document)
			throws SocialNetworkException {

		if (!setFor(requestedBy).contains(id)) {
			throw new SocialNetworkException(
					"Request source is invalid. Requested by: " + requestedBy
							+ " for id: " + id);
		} else {
			setFor(requestedBy).remove(id);
		}

        try {
            collectionFor(collectionName).update(
                    new BasicDBObject("_id", id), document);
        } catch (MongoException e) {
            System.out.println("Update error! Shouldn't run into this line. User id"+ document.get("userid"));
            // The document is bigger than 16 MB, set status to 4, decide what to do next.
            /*
            document.put("friends_status", 4);
            document.put("following_status", 4);
            document.put("friends", new HashSet<>());
            document.put("followers", new HashSet<>());
            // reset the document to empty then try again, this should work

            try {
                collectionFor(collectionName).update(new BasicDBObject("_id", id), document);
            } catch(MongoException ex) {
                System.out.println("Update fail again!!");
                System.out.println(ex);
            }
            */
            System.out.println(e);


        }
	}

	/* Modified by Yeqing Yan at Apr 13.
	* remove synchronized keyword to speed up id_queue processing time
	* */
	public boolean contains(
			Long id, 
			String fieldName, 
			String collectionName) 
			throws SocialNetworkException {
		
		return collectionFor(collectionName).
				find(new BasicDBObject(fieldName, id)).count() > 0;
	}

	/* Modified by Yeqing Yan remove the synchronized limit */
	public void  writeDocument(
			DBObject document,
			String collectionName) 
			throws SocialNetworkException {
		collectionFor(collectionName).insert(document);
	}

	private DBCollection collectionFor(
			String collectionName)
			throws SocialNetworkException {

		if (collectionName.equals(SOCIAL)) {
			return socialNetwork;
		} else if (collectionName.equals(USERS)) {
			return userProfiles;
		} else if (collectionName.equals(ID_QUEUE)) {
			return idQueue;
		} else {
			throw new SocialNetworkException("Invalid collection name: "
					+ collectionName);
		}
	}

	private synchronized boolean isDocumentCheckedOut(
			ObjectId id) {

		for (String name : documentsCheckedOut.keySet()) {
			if (documentsCheckedOut.get(name).contains(id)) {
				return true;
			}
		}
		return false;
	}

	private Set<ObjectId> setFor(
			String requestedBy) {

       /* increase the size limit from 10 to 100 */
       if (documentsCheckedOut.size() > 100) {
    	   System.out.println("documentsCheckedOut too big!");
    	   System.exit(1);
       }
		
		if (!documentsCheckedOut.containsKey(requestedBy)) {
			documentsCheckedOut.put(requestedBy, new HashSet<ObjectId>());
		}
		if (documentsCheckedOut.get(requestedBy).size() > 100) {
			System.out.println("Set too big: "+requestedBy);
			System.exit(1);
		}
		return documentsCheckedOut.get(requestedBy);
	}
	
}
