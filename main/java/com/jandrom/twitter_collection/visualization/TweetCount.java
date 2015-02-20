package com.jandrom.twitter_collection.visualization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

public class TweetCount {

	private File source;
	private File output;
	private Map<String, Integer> count;
	
	public TweetCount(String directory, String output) throws IOException {
		source = new File(directory);
		if (!source.isDirectory()) {
			throw new IOException("Not a directory.");
		}
		
		this.output = new File(output);
		this.count = new HashMap<>();
	}
	
	public void execute() {
		for (File file: source.listFiles()) {
			if (file.getName().endsWith("_usercount.csv")) {
				try {
					processFile(file);
				} catch (IOException ex) {
					//Shouldn't reach.
				}
			}
		}
		generateChart();
	}
	
	private void generateChart() {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		System.out.println(count.size());
		for (String hashtag: count.keySet()) {
			dataset.setValue(count.get(hashtag), "# Users" , hashtag);
		}
		JFreeChart chart = ChartFactory.createBarChart("Number of Tweets per Hashtag",
		  "Hashtag", "Number of Tweets", dataset, PlotOrientation.VERTICAL,
		   false, true, false);
		CategoryPlot plot = (CategoryPlot)chart.getPlot();
        CategoryAxis xAxis = (CategoryAxis)plot.getDomainAxis();
        xAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45);
		try {
		     ChartUtilities.saveChartAsJPEG(output, chart, 1200, 400);
		} catch (IOException e) {
		     System.err.println("Problem occurred creating chart.");
		}
	}
	
	public void processFile(File file) throws IOException {
		System.out.print(file.getName());
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String hashtag = extractHashtag(file.getName());
		String line;
		int c = 0;
		while ((line = reader.readLine()) != null) {
			c++;
		}
		reader.close();
		System.out.println(":   "+c);
		this.count.put(hashtag, c);
	}
	
	private String extractHashtag(String filename) {
		int underscore = filename.indexOf("_");
		return filename.substring(0, underscore);
	}
	
	public static void main(String[] args) throws IOException {
		(new UserCount("/Users/alex/Desktop/results2", "/Users/alex/Desktop/tweetCount.jpg")).execute();
	}
}
