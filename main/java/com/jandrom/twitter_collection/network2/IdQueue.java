package com.jandrom.twitter_collection.network2;

//import java.net.UnknownHostException;
//import java.util.concurrent.ConcurrentLinkedDeque;

import com.mongodb.BasicDBObject;
//import com.mongodb.Cursor;
//import com.mongodb.DB;
//import com.mongodb.DBCollection;
import com.mongodb.DBObject;
//import com.mongodb.MongoClient;

/* Modified by Yeqing Yan at Apr 26 */
// This class donot need to be runnable any more.
// public class IdQueue implements Runnable{
public class IdQueue {

	private SocialNetworkDB db;
	/* Modified by Yeqing Yan at Apr 26 */
	//private ConcurrentLinkedDeque<Long> ids;
	
	public IdQueue(SocialNetworkDB db) {
		this.db = db;
		/* Modified by Yeqing Yan at Apr 26 */
		//this.ids = new ConcurrentLinkedDeque<>();
	}

	/* Modified by Yeqing Yan at Apr 26 */
	// Disable run function
	// This class donot need to be runnable any more.
	/*
	@Override
	public void run() {
		Long currentId;
		while (!Thread.interrupted() || ids.isEmpty()) {
			
			while (!ids.isEmpty()) {
				
				currentId = ids.poll();
				if (checkIdNotExist(currentId)) {
					addId(currentId);
				}
			}
			
			try { Thread.sleep(20000); }
			catch (InterruptedException ex) { }
		}
	}

	
	public void batchAddIds(long[] ls) {
		for (long id: ls) {
			ids.add(id);
		}
	}
	*/

	// Modified by Yeqing Yan at APR 26
	// Change this function from private to public
	public void addId(Long id) {
		try {
			db.writeDocument(buildIdQueueObject(id), 
					SocialNetworkDB.ID_QUEUE);
		} catch (SocialNetworkException e) {
			System.out.println("IdQueueBuilder:\t"+e.getMessage());
			e.printStackTrace();
		}
	}
	
	private DBObject buildIdQueueObject(Long id) {
		DBObject doc = (new BasicDBObject("id", id));
		doc.put("done", 0);
		return doc;
	}

	// Modified by Yeqing Yan at APR 26
	// Change this function from private to public
	public boolean checkIdNotExist(Long id) {
		try {
			return !db.contains(id, "id", SocialNetworkDB.ID_QUEUE) && 
					!db.contains(id, "id",SocialNetworkDB.USERS);
		} catch (SocialNetworkException e) {
			//Shouldn't get here
			System.out.println("IdQueueBuilder:\t"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	/*
	public static void main(String[] args) throws UnknownHostException {
		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB("stream_store");
		DBCollection id_queue =  db.getCollection("id_queue");	
		DBCollection idQueue = db.getCollection("idQueue");
		Cursor cursor = idQueue.find(new BasicDBObject("moved", null));
		int count = 0;
		
		while (cursor.hasNext()) {
			BasicDBObject doc = (BasicDBObject) cursor.next();
			long id = doc.getLong("id");
			if (id_queue.count(new BasicDBObject("id", id)) == 0) {
				id_queue.insert(doc);
			}
			count++;
			if (count%50000 == 0) {
				System.out.println("count: "+count);
			}
		}
	}
	*/
}
