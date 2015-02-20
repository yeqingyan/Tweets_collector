package com.jandrom.twitter_collection.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.RectangleEdge;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class PolarityAnalyzer {

	private Map<String, Double> tweetCount;
	private Map<String, Double> polarityCount;
	private Map<String, Double> overallFeelingCount;
	private Map<String, Double> positiveCount;
	private Map<String, Double> negativeCount;
	private Set<String> hashtags;
	
	private DBCursor cursor;
	private final int POSITIVE = 0;
	private final int NEGATIVE = 1;
	
	public PolarityAnalyzer() throws UnknownHostException {

	    hashtags = generateHashtags();
	    tweetCount = new HashMap<>();
	    polarityCount = new HashMap<>();
	    positiveCount = new HashMap<>();
	    negativeCount = new HashMap<>();
	    overallFeelingCount = new HashMap<>();
	    setupMaps();
	    
	    
	    MongoClient client = new MongoClient("localhost");
	    DB db = client.getDB("stream_store");
        DBCollection col = db.getCollection("tweets");
        cursor = col.find();
	}
	
	public void execute() throws IOException {
		//int i = 0;
		while (cursor.hasNext()) {
			DBObject tweet = cursor.next();
			String text = (String) tweet.get("text");
			int[] polarity = polarity(text);
			/*if (polarity[0] > 0 || polarity[1] > 0) {
				System.out.println(""+polarity[POSITIVE]+": "+polarity[NEGATIVE]+"\t"+text);
				i++;
				if (i >= 10) {
					System.exit(1);
				}
			}*/
			
			for(Object obj: (BasicDBList) (((DBObject) 
					tweet.get("entities")).get("hashtags"))) {
				DBObject dbobj = (DBObject) obj;
				String hash = ((String) dbobj.get("text")).toLowerCase();
				if (hashtags.contains(hash)) {
					 addCount(tweetCount, hash, 1);
					 if (polarity[POSITIVE] > 0 || polarity[NEGATIVE] > 0) {
						 addCount(polarityCount, hash, 1);
					 }
					 if (polarity[POSITIVE] > 0) {
						 addCount(positiveCount, hash, 1);
					 }
					 if (polarity[NEGATIVE] > 0) {
						 addCount(negativeCount, hash, 1);
					 }
					 if (polarity[POSITIVE] > polarity[NEGATIVE]) {
						 addCount(overallFeelingCount, hash, 1);
					 }
				}
			}
		}
		printChart();
	}
	
	private int[] polarity(String text) {
		int polarity[] = {0,0};
		int count = countInstance(text, ":)");
		if (count > 0) {
			polarity[POSITIVE] += count;
		}
		count = countInstance(text, ": )");
		if (count > 0) {
			polarity[POSITIVE] += count;
		}
		count = countInstance(text, ":D");
		if (count > 0) {
			polarity[POSITIVE] += count;
		}
		count = countInstance(text, "=)");
		if (count > 0) {
			polarity[POSITIVE] += count;
		}
		count = countInstance(text, ":-)");
		if (count > 0) {
			polarity[POSITIVE] += count;
		}
		count = countInstance(text, ":(");
		if (count > 0) {
			polarity[NEGATIVE] += count;
		}
		count = countInstance(text, ": (");
		if (count > 0) {
			polarity[NEGATIVE] += count;
		}
		count = countInstance(text, ":-(");
		if (count > 0) {
			polarity[NEGATIVE] += count;
		}
		/*if (text.contains(":)") || text.contains(": )") || text.contains(":D")||
				text.contains("=)") || text.contains(":-)")) {
			polarity[POSITIVE]++;
		}*/
		/*if (text.contains(":(") || text.contains(": (") || text.contains(":-(")) {
			polarity [NEGATIVE]++;
		}*/
		return polarity;
	}
	
	private int countInstance(String text, String symbol) {
		int count = 0;
		int index = text.indexOf(symbol);
		while (index > 0) {
			count++;
			index = text.indexOf(symbol, index+1);
		}
		
		return count;
	}
	
	private void addCount(Map<String, Double> map, String key, int count) {
		if (map.containsKey(key)) {
			map.put(key, map.get(key) + count);
		} else {
			map.put(key, count * 1.0) ;
		}
	}
	
	private int count(String text, String emoticon) {
		int index = text.indexOf(emoticon);
		int count = 0;
		while (index >= 0) {
			index = text.indexOf(emoticon, index+1);
		}
		return count;
	}
	
	private void printChart() {
		System.out.printf("Hashtag: Tweets\tPolarity\tPositive\n");
		for (String hashtag : tweetCount.keySet()) {
			System.out.print(tweetCount.get(hashtag)+",");
			System.out.print(polarityCount.get(hashtag)+",");
			System.out.print(positiveCount.get(hashtag)+",");
			System.out.print(negativeCount.get(hashtag)+",");
			System.out.print(overallFeelingCount.get(hashtag)+",");
			System.out.println(hashtag);
			
			/*JFreeChart chart = ChartFactory.createBarChart(
					"Polarity", "Hashtag",
					"Number of Tweets/Polarity/Positive", dataset, PlotOrientation.VERTICAL,
					true, true, false);
			chart.getLegend().setPosition(RectangleEdge.TOP);
			CategoryPlot plot = (CategoryPlot) chart.getPlot();
			CategoryAxis xAxis = (CategoryAxis) plot.getDomainAxis();
			xAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45);
			try {
				File out = new File("/Users/alex/desktop/polarity.jpg");
				ChartUtilities.saveChartAsJPEG(out, chart, 1200, 700);
			} catch (IOException e) {
				System.err.println("Problem occurred creating chart.");
			}*/
			
		}
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
	
	private void setupMaps() {
		for (String s: hashtags) {
			tweetCount.put(s, 0.0);
			polarityCount.put(s, 0.0);
			positiveCount.put(s, 0.0);
			negativeCount.put(s, 0.0);
			overallFeelingCount.put(s, 0.0);
		}
	}

	public static void main(String[] args) throws UnknownHostException, IOException {
		(new PolarityAnalyzer()).execute();
	}
}
