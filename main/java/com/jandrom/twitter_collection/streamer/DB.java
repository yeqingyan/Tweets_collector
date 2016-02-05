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
                /* Modified by Yeqing Yan at Apr 27 Begin */
                DBObject tweet_doc = null;
                for (String json: tweetSet) {
                        /* Handle Limit notice message*/
                            tweet_doc = makeDBObject(json);
                            if (tweet_doc.containsField("limit") == true) {
                                System.out.println("Discard limit notice message: " + json);
                                continue;
                            } else {
                                list.add(tweet_doc);
                                //list.add(makeDBObject(json));
                            }
                    /* Modified by Yeqing Yan at Apr 27 End */
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
