package com.jandrom.twitter_collection.network2;

import java.util.HashSet;
import java.util.Set;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FriendsAndFollowersClient extends SocialNetworkClient {

	private boolean friends;
	private long currentIndex;
	private BasicDBObject currentDoc;
	private IdQueue idQueue;
	private Set<Long> accumulator;
	
	public FriendsAndFollowersClient(
			String name, 
			Twitter twitter,
			SocialNetworkDB db, 
			String readCollection, 
			String writeCollection,
			DBObject readQuery, 
			IdQueue idQueue, 
			int readCount,
			int rateLimit, 
			boolean friends
		) {

		super(name, twitter, db, readCollection, writeCollection, readQuery,
				readCount, rateLimit);
		this.friends = friends;
		this.currentIndex = -1;
		this.currentDoc = null;
		this.idQueue = idQueue;
		this.accumulator = new HashSet<>();
	}

	@Override
	public DBObject execRequest(DBObject doc) {
		if (currentDoc == null) {
			currentDoc = (BasicDBObject) doc;
		}

		try {
			IDs ids = call();
			for (long i : ids.getIDs()) {
				accumulator.add(i);
			}
			idQueue.batchAddIds(ids.getIDs());

			if (ids.hasNext()) {
                // Mongodb don't allow a singer document bigger than 16MB
                // If the accumulator size is bigger than 1000000, stop collection more data
                if (accumulator.size() > 1000000) {
                    System.out.println("User id "+ currentDoc.get("userid") + " have more than 1000000 followers ro friends, stop collecting.");
                    add(accumulator);
                    return reset(false);
                }
				currentIndex = ids.getNextCursor();
				return null;
			} else {
				add(accumulator);
				return reset(false);
			}
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			log(e);
			//e.printStackTrace();
			return reset(true);
		}
	}
	
	private void add(Set<Long> ids) {
	    if (friends) {
	    	currentDoc.put("friends", ids);
	    	currentDoc.put("friends_status", 2);
	    } else {
	    	currentDoc.put("followers", ids);
	    	currentDoc.put("following_status", 2);
	    }
	}
	
	private IDs call() throws TwitterException {
		if (friends) {
			/*return twitter.getFriendsIDs(
					(long) currentDoc.getLong("userid"),
					currentIndex);*/
            return twitter.getFriendsIDs(
                    twitter.showUser((long) currentDoc.getLong("userid")).getScreenName(),
                    currentIndex);
		} else {
			/*return twitter.getFollowersIDs(
					(long) currentDoc.getLong("userid"), 
					currentIndex);*/
            return twitter.getFollowersIDs(
                    twitter.showUser((long) currentDoc.getLong("userid")).getScreenName(),
                    currentIndex);
		}
	}
	
	private DBObject reset(boolean error) {
		DBObject doc = currentDoc;
		currentDoc = null;
		currentIndex = -1;
		accumulator = new HashSet<>();
		if (error) {
			String key = friends? "friends_status" : "following_status";
			doc.put(key, 1);
		}
		return doc;
	}

}
