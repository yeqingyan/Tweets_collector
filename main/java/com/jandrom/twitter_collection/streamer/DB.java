package com.jandrom.twitter_collection.streamer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class DB {

	private JSONParser parser;
	private MongoClient mongo;
	private DBCollection tweets;
	private Set<String> tweetSet;

	public DB() throws UnknownHostException {
		mongo = new MongoClient("localhost");
		parser = new JSONParser();
		tweets = mongo.getDB("stream_store").getCollection("tweets");
		tweetSet = new HashSet<>();
	}

	public void close() {
		batchWrite();
		mongo.close();
	}

	public synchronized void writeStatus(String tweet) {
		tweetSet.add(tweet);
		if (tweetSet.size() > 100) {
			batchWrite();
			tweetSet.clear();
		}
	}

	private void batchWrite() {
		if (tweetSet.isEmpty()) {
			return;
		}

		List<DBObject> list = new ArrayList<>();
		for (String json: tweetSet) {
			list.add(makeDBObject(json));
		}
		tweets.insert(list);
	}

	private DBObject makeDBObject(String json) {
		try {
			return new BasicDBObject((Map) parser.parse(json));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
}
