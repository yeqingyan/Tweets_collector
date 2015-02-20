package com.jandrom.twitter_collection.user_db;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class BuildNeededIds {
	
	public static final String ID = "id";
	
	public static void main(String[] args) throws UnknownHostException {
		System.out.println("Starting");
		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB("stream_store");
		DBCollection social_network = db.getCollection("social_network");
		DBCollection id_queue = db.getCollection("id_queue");
		Cursor friendsCursor = social_network.find(new BasicDBObject("friends_status", 2));
		Cursor followersCursor = social_network.find(new BasicDBObject("following_status", 2));
		processCursor(friendsCursor,"friends", id_queue);
		processCursor(followersCursor, "followers", id_queue);
		System.out.println("Ending");
	}
	
	private static void processCursor(Cursor cursor, String key, DBCollection id_queue) {
		int count = 0;
		int ids = 0;
		while (cursor.hasNext()) {
			count++;
			BasicDBObject doc = (BasicDBObject) cursor.next();
			BasicDBList list = (BasicDBList) doc.get(key);
			if (list == null || list.size() == 0) {
				continue;
			}
			
			for (Object id: list) {
				if (!hasId((Long) id, id_queue)) {
					ids++;
					id_queue.insert(buildDocFor(((Long) id)));
				}
			}
		}
		
		System.out.println(key+" has "+count+" matching profiles with a total "+ids+" ids added.");
	}
	
	private static DBObject buildDocFor(Long id) {
	    BasicDBObject doc = new BasicDBObject(ID, id);
	    doc.put("done", 0);
	    return doc;
	}
	
	private static boolean hasId(Long id, DBCollection id_queue) {
		return id_queue.count(new BasicDBObject(ID, id)) > 0;
	}

}
