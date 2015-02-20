package com.jandrom.twitter_collection.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.RectangleEdge;

public class PostProcessor {
	public static long total = 0L;

	private File file;
	private String directory;
	private Map<String, HashtagProfile> profileMap;
	private Map<String, Integer> tweetCount;
	private Map<String, Map<Long, Integer>> userCount;

	public PostProcessor(String filename, String directory) {
		this.file = new File(filename);
		this.directory = directory;
		this.profileMap = new HashMap<>();
		tweetCount = new HashMap<>();
		userCount = new HashMap<>();
	}

	public void execute() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		while ((line = reader.readLine()) != null) {
			processLine(line);
		}

		saveTweetAndUserCount();

		// StringBuilder builder = new StringBuilder();
		// for (String hashtag: profileMap.keySet()) {
		// profileMap.get(hashtag).save();
		// builder.append(hashtag).append(",")
		// .append(profileMap.get(hashtag).count()).append("\n");
		// }

		// File file = new File(directory+"/tweetcount.csv");
		// FileWriter writer = new FileWriter(file);
		// writer.write(builder.toString());
		// writer.close();
	}

	/*
	 * private void processLine(String line) { String[] split = line.split(",");
	 * Long userid = Long.parseLong(split[0]); boolean retweet =
	 * Boolean.getBoolean(split[1].trim()); String date = processDate(split[2]);
	 * String[] hashtags = getHashtags(split);
	 * 
	 * for (String hashtag: hashtags) { HashtagProfile profile =
	 * getHashtagProfile(hashtag); profile.addUser(userid);
	 * profile.addDay(date); if (retweet) { profile.addRetweet(); } }
	 * 
	 * }
	 */

	private void processLine(String line) {
		String[] split = line.split(",");
		Long userid = Long.parseLong(split[0]);
		boolean retweet = Boolean.getBoolean(split[1].trim());
		String date = processDate(split[2]);
		String[] hashtags = getHashtags(split);

		for (String hashtag : hashtags) {
			addTweet(hashtag);
			addUsertoTweet(hashtag, userid);
		}
	}

	private void addTweet(String hashtag) {
		if (tweetCount.containsKey(hashtag)) {
			tweetCount.put(hashtag, tweetCount.get(hashtag) + 1);
		} else
			tweetCount.put(hashtag, 1);
	}

	private void addUsertoTweet(String hashtag, long userid) {
		Map<Long, Integer> map;
		if (userCount.containsKey(hashtag)) {
			map = userCount.get(hashtag);
		} else {
			map = new HashMap<>();
		}

		if (map.containsKey(userid)) {
			map.put(userid, map.get(userid) + 1);
		} else
			map.put(userid, 1);

		userCount.put(hashtag, map);
	}

	private void saveTweetAndUserCount() {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		for (String hashtag : tweetCount.keySet()) {
			dataset.setValue(tweetCount.get(hashtag), "TweetCount", hashtag);
			dataset.setValue(userCount.get(hashtag).size(), "UserCount",
					hashtag);
		}

		JFreeChart chart = ChartFactory.createBarChart(
				"Total Tweets and Total Users", "Hashtag",
				"Number of Tweets/Users", dataset, PlotOrientation.VERTICAL,
				true, true, false);
		chart.getLegend().setPosition(RectangleEdge.TOP);
		CategoryPlot plot = (CategoryPlot) chart.getPlot();
		CategoryAxis xAxis = (CategoryAxis) plot.getDomainAxis();
		xAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45);
		try {
			File out = new File("/Users/alex/desktop/totaltweetstotalusers.jpg");
			ChartUtilities.saveChartAsJPEG(out, chart, 1200, 700);
		} catch (IOException e) {
			System.err.println("Problem occurred creating chart.");
		}

	}

	private String[] getHashtags(String[] s) {
		String[] array = new String[s.length - 4];
		for (int i = 0; i < array.length; i++) {
			array[i] = s[i + 4];
		}
		return array;
	}

	private String processDate(String s) {
		String[] split = s.split(" ");
		String month = split[1];
		String day = split[2];
		String year = split[5];
		return month + "/" + day + "/" + year;
	}

	private HashtagProfile getHashtagProfile(String s) {
		if (this.profileMap.containsKey(s)) {
			return this.profileMap.get(s);
		} else {
			HashtagProfile profile = new HashtagProfile(s);
			this.profileMap.put(s, profile);
			return profile;
		}
	}

	private class HashtagProfile {

		private String name;
		private Map<Long, Long> userCount;
		private Map<String, Long> dailyCount;
		private Long tweetCount;
		private Long retweetedCount;

		public HashtagProfile(String hashtag) {
			this.name = hashtag;
			userCount = new HashMap<>();
			dailyCount = new HashMap<>();
			tweetCount = 0L;
			retweetedCount = 0L;
		}

		public String count() {
			return "" + tweetCount;
		}

		public void addUser(long userid) {
			tweetCount++;
			if (userCount.containsKey(userid)) {
				userCount.put(userid, userCount.get(userid) + 1);
			} else {
				userCount.put(userid, 1L);
			}
		}

		public void addDay(String day) {
			if (dailyCount.containsKey(day)) {
				dailyCount.put(day, dailyCount.get(day) + 1);
			} else {
				dailyCount.put(day, 1L);
			}
		}

		public void addRetweet() {
			retweetedCount++;
		}

		public void save() throws IOException {
			BufferedWriter writer = new BufferedWriter(new PrintWriter(
					directory + "/" + name + "_usercount.csv"));
			StringBuilder builder = new StringBuilder();
			int count = 0;
			for (Long key : userCount.keySet()) {
				builder.append(key).append(",").append(userCount.get(key))
						.append("\n");
				count++;
				if (count == 10000) {
					writer.write(builder.toString());
					writer.flush();
					builder = new StringBuilder();
				}
			}
			writer.write(builder.toString());
			writer.close();

			writer = new BufferedWriter(new PrintWriter(directory + "/" + name
					+ "_daycount.csv"));
			builder = new StringBuilder();
			count = 0;
			for (String key : dailyCount.keySet()) {
				builder.append(key).append(",").append(dailyCount.get(key))
						.append("\n");
			}
			writer.write(builder.toString());
			writer.close();
		}
	}

	public static void main(String[] args) throws IOException {
		PostProcessor processor = new PostProcessor(
				"/Users/alex/Desktop/out.csv",
				"/Users/alex/Desktop/results2");
		processor.execute();
		System.out.println("total: " + PostProcessor.total);
	}
}
