package com.jandrom.twitter_collection.network2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class SocialMain {
	
	private static List<Thread> threads;

	public static void main(String[] args) throws IOException, ParseException {
		String file = args[0];//"/Users/alex/desktop/socnet.json";
		Map<String, Twitter> twitters = getTwitters(file);
		SocialNetworkDB db = new SocialNetworkDB();
		IdQueue idQueue = new IdQueue(db);
		threads = new ArrayList<>();
		
		for (String key: twitters.keySet()) {
			Twitter t = twitters.get(key);
			
			Thread t1 = new Thread(new FriendsAndFollowersClient(
					key+"-friends", 
					t,
					db, 
					SocialNetworkDB.SOCIAL, 
					SocialNetworkDB.SOCIAL, 
					new BasicDBObject("friends_status", 0), 
					idQueue,
					10, 
					15, 
					true)
			);
			t1.start();
			threads.add(t1);
			
			Thread t2 = new Thread(new FriendsAndFollowersClient(
					key+"-followers", //CHANGE
					t, 
					db, 
					SocialNetworkDB.SOCIAL, 
					SocialNetworkDB.SOCIAL, 
					new BasicDBObject("following_status", 0), //CHANGE
					idQueue,
					10, 
					15, 
					false) //CHANGE
			);
			t2.start();
			threads.add(t2);
		}
	
		Thread idQueueThread = new Thread(idQueue);
		idQueueThread.start();
		
		while (!done()) {
			try { Thread.sleep(30000); }
			catch (InterruptedException ex) {}
		}
		
		for (Thread t: threads) {
			t.interrupt();
		}
		
		while (idQueueThread.isAlive()) {
			try { Thread.sleep(10000); }
			catch (InterruptedException ex) {}
		}
	}

	
	public static Map<String, Twitter> getTwitters(String filename) throws IOException, ParseException {
		File file = new File(filename);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuilder builder = new StringBuilder();
		String line;
		Map<String, Twitter> map = new HashMap<>();
		
		
		while ((line = reader.readLine()) != null) {
			builder.append(line);
		}
		reader.close();
		
		JSONObject json = (JSONObject) (new JSONParser()).parse(builder.toString());
		ConfigurationBuilder config = new ConfigurationBuilder();
		config.setJSONStoreEnabled(true);
		TwitterFactory factory = new TwitterFactory(config.build());
		for (Object obj: (JSONArray) json.get("clients")) {
			JSONObject jsonobj = (JSONObject) obj;
			System.out.println(jsonobj.toJSONString());
			AccessToken access = new AccessToken((String) jsonobj.get("token"),
					(String) jsonobj.get("token_secret"));
			Twitter t = factory.getInstance();
			t.setOAuthConsumer((String) jsonobj.get("key"), 
					(String) jsonobj.get("secret"));
			t.setOAuthAccessToken(access);
			map.put((String) jsonobj.get("name"),t);
		}
		//System.exit(0);
		return map;
	}
	
	public static boolean done() {
		return (new File("die.txt")).exists();
	}
}
