package com.jandrom.twitter_collection.streamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class StreamerMain {

	public static Authentication authentication;
	public static List<String> hashtags;

	public static void main(String[] args) throws ParseException, IOException {
		init(args[0]);
		//init("/Users/alex/Desktop/config.json");
		Streamer streamer = new Streamer(hashtags, authentication);
		streamer.start();

		while (stayAlive()) {
			try { Thread.sleep(20000); }
			catch (InterruptedException ex) {}

		}

		System.out.println("Ending .....");
		streamer.end();
		System.out.println("Exiting");
	}

	public static void init(String filename) throws ParseException, IOException {
		JSONObject doc = (JSONObject) (new JSONParser()).parse(readFile(filename));
		hashtags = new ArrayList<>();
		for (Object o: (JSONArray) doc.get("hashtags")) {
			String asString = (String) o;
			if (asString.charAt(0) == '#') {
				asString = asString.substring(1);
			}
			hashtags.add((String) o);
		}

		authentication = new OAuth1(
				(String) doc.get("consumer_key"),
				(String) doc.get("consumer_secret"),
				(String) doc.get("token"),
				(String) doc.get("token_secret")
		);
	}

	private static String readFile(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		StringBuilder builder = new StringBuilder();
		String line = reader.readLine();
		while (line != null) {
			builder.append(line);
			line = reader.readLine();
		}
		reader.close();
		return builder.toString();
	}

	private static boolean stayAlive() {
		return !(new File("die.txt").exists());
	}
}
