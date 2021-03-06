package com.jandrom.twitter_collection.network2;

import java.util.Date;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;

import twitter4j.Twitter;

public abstract class SocialNetworkClient implements Runnable {

	private int rateLimit;
	private String name;
	protected Twitter twitter;

	private SocialNetworkDB db;
	private String readCollection;
	private String writeCollection;
	private DBObject readQuery;
	private int readCount;

	private long windowStart;
	private long windowEnd;
	private int numReq;

	public SocialNetworkClient(
			String name,
			Twitter twitter,
			SocialNetworkDB db,
			String readCollection,
			String writeCollection,
			DBObject readQuery,
			int readCount,
			int rateLimit
			) {
		
		this.name = name;
		this.twitter = twitter;
		this.db = db;
		this.readCollection = readCollection;
		this.writeCollection = writeCollection;
		this.readQuery = readQuery;
		this.readCount = readCount;
		this.rateLimit = rateLimit;
		/* NOTE by Yeqing at Apr 13 Begin
		   The start and end windows time setup too early,
		   the query is not start yet, should after getDocument() function.
		   Keep the initialization here, but the start and end will update after getDocuments()
		   NOTE by Yeqing End*/
		this.windowStart = System.currentTimeMillis();
		this.windowEnd = windowStart + 1000 * 60 * 16; // 16 mins after start
		this.numReq = 0;
	}

	public abstract DBObject execRequest(DBObject doc);

	@Override
	public void run() {
		log("Starting");
		boolean done = false;

		while (!Thread.interrupted() && !done) {
			try {
				Set<DBObject> currentDocs = getDocuments();
				if (currentDocs == null) {
					done = true;
					continue;
				} else if (currentDocs.isEmpty()) {
					/* Add by Yeqing Yan */
					// Note this could rarely happened at the beginning of the program.
					// Consider this scenario:
					// Documents following_status=0 is checkout by Followers threads first.
					// Then Friends thread can not checkout these documents at the same time.
					// Because each document can only be checked out at once.
					// So it is fine when some thread going to sleep at the first 15 minutes.
                    System.out.println("currentDocs is empty, sleep 20 min.");
					sleep(1000*60*20);
					continue;
				}
				/* NOTE by Yeqing at Apr 13 Begin
				   After getDocument function finished
				   Update start and end window here to avoid rate limit error
				 */
				this.windowStart = System.currentTimeMillis();
				this.windowEnd = windowStart + 1000 * 60  * 16;
                /* NOTE by Yeqing End*/
				for (DBObject doc : currentDocs) {
					DBObject insertDoc = null;
					ObjectId id = (ObjectId) doc.get("_id");
					while (insertDoc == null) {
						insertDoc = execRequest(doc);
						checkHold();
					}
					if (id != insertDoc.get("_id")) {
						System.out.println("\n\nname: Insert id != existing id.");
					}
                    try {
                        write(id, insertDoc);
                    } catch (Exception e) {
                        // Write error, the document might too big
                        System.out.println("\nWrite error. id is "+id);
                    }
				}
				
			} catch (Exception e) {
                System.out.println("Pay attentation! Met some error!");
				log(e);
				checkHold();
			}
		}
		
		System.out.println(name+": exiting.");
	}

	private Set<DBObject> getDocuments() {
		try {
			return db.checkoutDocumentsMatching(
					name, 
					readCollection, 
					readQuery,
					readCount);
		} catch (SocialNetworkException e) {
			// Shouldn't reach: only if readCollection
			// is invalid.
			log(e);
			return null;
		}
	}

	private void checkHold() {
		numReq++;
		if (numReq % rateLimit == 0) {
			log("Holding: Read " + numReq);
			windowStart = System.currentTimeMillis();
			while (windowStart < windowEnd) {
				sleep(30000);
				windowStart = System.currentTimeMillis();
			}

			windowEnd = windowStart + 1000 * 60 * 16;
		}
	}

	protected void log(String logtext) {
		System.out.println("\n\nThread: " + name + ".\t" + (new Date()));
		System.out.println(logtext);
	}

	protected void log(Exception e) {
		System.out.println("\n\nThread Exception: " + name + ".\t"
				+ (new Date()));
		System.out.println(e.getMessage());
	}

	private void sleep(long time) {
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < time) {
			try {
				Thread.sleep(time);
			} catch (InterruptedException ex) {
			}
		}
	}
	
	private void write(ObjectId id, DBObject doc) {
		try {
			db.checkinDocument(
					name, 
					writeCollection, 
					id, 
					doc);
		} catch (SocialNetworkException e) {
			// Shouldn't reach here on bad collection name
			// Only if user cannot check in item.
			log(e);
		}
	}

}
