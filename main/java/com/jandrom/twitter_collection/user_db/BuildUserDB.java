package com.jandrom.twitter_collection.user_db;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class BuildUserDB {
	
	public static void main(String[] args) throws UnknownHostException {
		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB("stream_store");
		DBCollection tweets = db.getCollection("tweets");
		DBCollection userProfiles = db.getCollection("user_profiles");
		
		Cursor cursor = tweets.find();
		DBObject currentDoc;
		BasicDBObject currentUser;
		Long currentUserId;
		int i = 0;
		while (cursor.hasNext()) {
			currentDoc = cursor.next();
			currentUser = (BasicDBObject) currentDoc.get("user");
			currentUserId =  currentUser.getLong("id");
			if (!userProfiles.find(
					userQuery(currentUserId)).hasNext()) {
				userProfiles.insert(currentUser);
			} else {
				i++;
				if (i%1000 == 0) {
					System.out.println("Found matches: "+i);
				}
			}
		}
	}
	
	public static DBObject userQuery(Long id) {
		return new BasicDBObject("id", id);
	}
}
