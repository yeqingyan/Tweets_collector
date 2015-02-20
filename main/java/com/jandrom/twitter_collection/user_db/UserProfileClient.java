package com.jandrom.twitter_collection.user_db;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import com.jandrom.twitter_collection.network2.SocialMain;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class UserProfileClient implements Runnable {
	
	private static DBCollection id_queue;
	private static DBCollection user_profiles;
	
	private JSONParser parser;
	private String name;
	private Twitter twitter;
	private int count;
	private long nextStart;
	private int total;
	
	public UserProfileClient(String name, Twitter t) {
		this.twitter = t;
		
		this.name = name;
		this.parser = new JSONParser();
		this.count = 0;
		this.total = 0;
		this.nextStart = System.currentTimeMillis() + 1000 * 60 * 16;
	}
	
	public void run() {
		boolean done = false;
        long[] result = null;

		while (!Thread.interrupted() && !done) {
			try {
                result = getUsers();
                if (result.length == 0) {
                    log("No item is Id_queue need to query! Going to sleep\n");
                    // Make Thread go to sleep quickly
                    count = 15;
                } else {
                    insertResult(twitter.lookupUsers(result));
                }
			} catch (TwitterException e) {
			    System.out.println(name+":\t"+e.getMessage()+"\n");
			} finally {
				count++;
				done = checkSleep();	
			}
		}
	}
	
	private boolean checkSleep() {
		if (count >= 15) {
			while (System.currentTimeMillis() < nextStart) {
				try { Thread.sleep(30000); }
				catch (InterruptedException ex) { return true; }
			}
			nextStart = System.currentTimeMillis() + 1000 * 60 * 16;
			total += count;
			log("read "+total);
			count = 0;
		}
		return false;
	}
	
	private void insertResult(ResponseList<User> users) {
		for (User user: users) {
			TwitterObjectFactory.getRawJSON(user);
			try {
				user_profiles.insert(
						new BasicDBObject(
						(Map) parser.parse(
						TwitterObjectFactory.getRawJSON(user))));
			} catch (ParseException e) {
				//shouldn't get here.
			}
		}
	}
	
	private void log(String text) {
		System.out.println(name+": \t"+text+"\n");
	}
	
	private static synchronized long[] getUsers() {
		Cursor cursor = id_queue.find(new BasicDBObject("done", 0)).limit(100);
		List<Long> list = new ArrayList<>();
		
		while (cursor.hasNext()) {
			BasicDBObject doc = (BasicDBObject) cursor.next();
			doc.put("done", 1);
			Long id = doc.getLong("id");
			list.add(id);
			id_queue.update(new BasicDBObject("id", id), doc);
		}
		
		long[] result = new long[list.size()];
		for (int i = 0; i < list.size(); i++) {
			result[i] = list.get(i);
		}

		return result;
	}
	
	/*
	 *  DRIVER PROGRAM
	 */

	private static Map<String, Twitter> map;
	private static Thread[] threads;
	
	public static void main(String args[]) throws UnknownHostException, IOException, ParseException {
		init(args[0]);
	
		while (!done()) {
			try { Thread.sleep(30000); }
			catch (InterruptedException ex) {}
		}
		
		for (Thread t: threads) {
			t.interrupt();
		}
	}
	
	private static void init(String confile) throws UnknownHostException, IOException, ParseException {
		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB("stream_store");
		id_queue =  db.getCollection("id_queue");	
		user_profiles = db.getCollection("user_profiles");
		
		map = SocialMain.getTwitters(confile);
		threads = new Thread[map.keySet().size()];
		int i = 0;
		for (String key: map.keySet()) {
			UserProfileClient cl = new UserProfileClient(key, map.get(key));
		    threads[i] = new Thread(cl);
		    threads[i].start();
		}
	}
	
	private static boolean done() {
		return (new File(System.getProperty("user.dir")+"/die.txt")).exists();
	}
	 
}
