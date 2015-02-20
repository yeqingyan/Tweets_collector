package com.jandrom.twitter_collection.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class DataCollector {
	
	private DBCollection collection;
	private File file;
	private Set<String> hashtags;

	public DataCollector(String database, String collection, String file) throws UnknownHostException {
		MongoClient client = new MongoClient("localhost");
		DB db = client.getDB(database);
		this.collection = db.getCollection(collection);	
		this.file = new File(file);
		this.hashtags = generateHashtags();
	}
	
	public void execute() throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		DBCursor cursor = collection.find();
		StringBuilder builder = new StringBuilder();
		int count = 0;
		int print = 0;
		
		
		while (cursor.hasNext()) {
			DBObject object = cursor.next();
			builder.append(analyze(object));
			count++;
			print++;
			
			if (count == 1000) {
				count = 0;
				writer.write(builder.toString());
				writer.flush();
				builder = new StringBuilder();
			}
			if (print%50000 == 0) {
				System.out.println(""+print);
			}
		}
		
		writer.close();
	}
	
	private String analyze(DBObject object) {
		StringBuilder builder = new StringBuilder();
		builder.append(((DBObject) object.get("user")).get("id")).append(",");
		builder.append(""+(object.get("retweeted_status") != null)).append(",");
		builder.append(object.get("created_at")).append(",");
		builder.append(object.get("timestamp_ms")).append(",");
		
		DBObject entities = (DBObject) object.get("entities");
		BasicDBList hashtags = (BasicDBList) entities.get("hashtags");
		for (Object o: hashtags) {
			DBObject h = (DBObject) o;
			String hashtag = ((String) h.get("text")).toLowerCase();
			if (this.hashtags.contains(hashtag)) {
				builder.append(hashtag.toString()).append(",");
			}
		}
		return builder.append("\n").toString();
	}
	
	private Set<String> generateHashtags() {
		Set<String> set = new HashSet<>();
		set.add("atoz".toLowerCase());
		set.add("theaffair".toLowerCase());
		set.add("badjudge".toLowerCase());
		set.add("blackishABC".toLowerCase());
		set.add("constantine".toLowerCase());
		set.add("cristela".toLowerCase());
		set.add("theflash".toLowerCase());
		set.add("gotham".toLowerCase());
		set.add("gracepoint".toLowerCase());
		set.add("happyland".toLowerCase());
		set.add("howtogetawaywithmurder".toLowerCase());
		set.add("janethevirgin".toLowerCase());
		set.add("kingdom".toLowerCase());
		set.add("madamsecretary".toLowerCase());
		set.add("marryme".toLowerCase());
		set.add("themccarthys".toLowerCase());
		set.add("mulaney".toLowerCase());
		set.add("mysteriesoflaura".toLowerCase());
		set.add("ncisnola".toLowerCase());
		set.add("redbandsociety".toLowerCase());
		set.add("scorpion".toLowerCase());
		set.add("selfieabc".toLowerCase());
		set.add("stalker".toLowerCase());
		set.add("survivorsremorse".toLowerCase());
		set.add("utopia".toLowerCase());
		set.add("znation".toLowerCase());
		set.add("blackish".toLowerCase());
		return set;
	}
	
	public static void main(String[] args) throws  IOException {
		DataCollector collector = new DataCollector("stream_store", "tweets", "/home/ec2-user/social/output.csv");
		collector.execute(); 
	}
}
