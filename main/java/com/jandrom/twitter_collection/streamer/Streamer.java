package com.jandrom.twitter_collection.streamer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;

public class Streamer {

	private Thread thread;
	private List<Thread> consumers;
	private LinkedBlockingQueue<String> queue;

	private List<String> hashtags;
	private Authentication authentication;

	private DB db;

	public Streamer(List<String> hashtags, Authentication authentication) throws UnknownHostException {
		consumers = new ArrayList<>();
		this.hashtags = hashtags;
		this.authentication = authentication;
		this.db = new DB();
	}


	public synchronized String nextStatus() {
		if (queue.isEmpty()) {
			return null;
		} else {
			return queue.poll();
		}
	}


	public void connect() {
		queue = new LinkedBlockingQueue<String>(100000);
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(hashtags);

		(new ClientBuilder())
				.hosts(hosebirdHosts)
				.authentication(authentication)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(queue))
				.build()
				.connect();
	}

	public void start() throws UnknownHostException {
		this.connect();

		for (int i = 0; i < 3; i++) {
			Thread t = new Thread(new StreamConsumer(this, db));
			consumers.add(t);
			System.out.println("Starting "+t.toString()+"......");
			t.start();
		}
	}

	public void end() {
		while (!queue.isEmpty()) { //queue is not empty
			break;
		}

		System.out.println("Queue is empty ...");

		for (Thread t: consumers) {
			System.out.println("Interrupting "+t+" .....");
			t.interrupt();
			while (t.isAlive()) {
				sleep(5);
			}
			System.out.println(t.toString()+" is dead.");
		}

		System.out.println("Flushing write queue.... ");
		db.close();
	}

	public static boolean sleep(int sec) {
		long end = System.currentTimeMillis() + sec*1000;
		while (System.currentTimeMillis() < end) {
			try { Thread.sleep(10000); }
			catch (InterruptedException ex) { return false; }
		}
		return true;
	}

	private class StreamConsumer implements Runnable {

		private Streamer streamer;
		private DB db;
		private int tweetsConsumed;

		public StreamConsumer(Streamer streamer, DB db) {
			this.db = db;
			this.streamer = streamer;
			this.tweetsConsumed = 0;
		}

		@Override
		public void run() {
			String json;
			boolean keepRunning = true;
			while (!Thread.interrupted() && keepRunning) {
				json = streamer.nextStatus();
				if (json == null) {
					keepRunning = sleep(2);
					continue;
				}
				else {
					db.writeStatus(json);
					tweetsConsumed++;
				}
			}

			System.out.println("Consumed "+tweetsConsumed+" tweets.");
		}
	}
}
