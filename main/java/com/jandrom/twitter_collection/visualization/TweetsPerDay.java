package com.jandrom.twitter_collection.visualization;

import java.util.List;
import java.awt.Color;
import java.awt.Paint;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.LegendItem;
import org.jfree.chart.LegendItemSource;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.block.BlockContainer;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleEdge;

public class TweetsPerDay {

	private File source;
	private String output;
	private Map<String, Map<String, Integer>> overallMap;
	private Map<String, PriorityQueue<Date>> overallQueue;
	private Map<String, XYSeries> timeSeries;

	private static Map<String, Integer> convert;

	static {
		convert = new HashMap<>();
		convert.put("Oct", 10);
		convert.put("Nov", 11);
		convert.put("Dec", 12);
		convert.put("Sep", 9);
	}

	public TweetsPerDay(String source_dir, String output_dir)
			throws IOException {
		this.source = new File(source_dir);
		this.output = output_dir;

		if (!source.isDirectory()) {
			throw new IOException("Not directories.");
		}
		this.overallMap = new HashMap<>();
		this.overallQueue = new HashMap<>();
		this.timeSeries = new HashMap<>();
	}

	public void execute() throws IOException {
		for (File file : source.listFiles()) {
			// if (file.getName().contains("getawaywithmurder_day")
			// || file.getName().contains("utopia_day")) {
			if (file.getName().endsWith("_daycount.csv")) {
				processFile(file);
			}
		}
		
		saveFiles();
	}

	private void processFile(File f) throws IOException {
		PriorityQueue<Date> dateQueue = new PriorityQueue<>();
		Map<String, Integer> dateCount = new HashMap<>();
		BufferedReader reader = new BufferedReader(new FileReader(f));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] split = line.split(",");
			String day = split[0];
			int count = Integer.parseInt(split[1]);
			dateCount.put(day, count);
			dateQueue.offer(datify(day));
		}
		reader.close();

		String name = f.getName();
		int underscore = name.indexOf("_");
		String hashtag = name.substring(0, underscore);
		overallMap.put(hashtag, dateCount);
		overallQueue.put(hashtag, dateQueue);

		XYSeries dataset = new XYSeries(hashtag);
		int i = 1;
		String last = "";
		while (!dateQueue.isEmpty()) {
			String s = dateQueue.poll().toString();
			if (i == 1) {
				System.out.println(s);
				System.exit(1);
			}
			if (i > 1) {
				i = fillTo(last, s, dataset, i);
			}
			last = s;
			dataset.add(i++, dateCount.get(s));
		}
		this.timeSeries.put(hashtag, dataset);
		/*XYSeriesCollection collection = new XYSeriesCollection(dataset);
		JFreeChart chart = ChartFactory.createXYLineChart(
				"Number of Tweets by Day for " + hashtag, "Day",
				"Number of Tweets", collection, PlotOrientation.VERTICAL, false,
				true, false);
		try {
			File out = new File(output + "/" + hashtag + ".jpg");
			ChartUtilities.saveChartAsJPEG(out, chart, 1200, 400);
		} catch (IOException e) {
			System.err.println("Problem occurred creating chart.");
		}*/

	}
	
	private int fillTo(String start, String to, XYSeries dataset, int i) {
		String[] splitStart = start.split("/");
		String[] splitTo  = to.split("/");
		int monthStart = 9;
		int monthEnd = 9;
		if (splitStart[0].equals("Oct")) {
			monthStart = 10;
		} else if (splitStart[0].equals("Nov")) {
			monthStart = 11;
		}
		if (splitTo[0].equals("Oct")) {
			monthEnd = 10;
		} else if (splitTo[0].equals("Nov")) {
			monthEnd = 11;
		}
		int dayStart = Integer.parseInt(splitStart[1])+1;
		int dayEnd = Integer.parseInt(splitTo[1]);
		
		while (monthStart <= monthEnd) {
		  	if (monthStart == 10 && dayStart == 32) {
		  		monthStart = 11;
		  		dayStart = 1;
		  	} else if (monthStart == 9 && dayStart == 31) {
		  		monthStart = 10;
		  		dayStart = 1;
		  	}
		  	
		  	if (monthStart == monthEnd && dayStart == dayEnd) {
		  		return i;
		  	}
		  	
		  	dataset.add(i++, 1);
		  	dayStart++;
		}
		return i;
	}
	
	private void saveFiles() {
		String[] highrollers = {"gotham", "theflash", "redbandsociety", "howtogetawaywithmurder"};
		printFile("/Users/alex/Desktop/tweetsovertime/highvolume.jpg", "High Volume Shows", highrollers);
		
		String[] medium1 = {"scorpion", "utopia", "blackishabc", "stalker", "janethevirgin"};
		printFile("/Users/alex/Desktop/tweetsovertime/mediumvolume1.jpg", "Medium Volume Shows 1", medium1);
		
		String[] medium2 = {"selfieabc", "constantine", "happyland", "mysteriesoflaura", "gracepoint"};
		printFile("/Users/alex/Desktop/tweetsovertime/mediumvolume2.jpg", "Mediume Volume Shows 2", medium2);
		
		String[] low1 = {"badjudge", "themccarthys", "theaffair", "mulaney", "ncisnola", "znation"};
		printFile("/Users/alex/Desktop/tweetsovertime/lowvolume1.jpg", "Low Volume Shows 1", low1);
		
		String[] low2 = {"kingdom", "madamsecretary", "cristela", "atoz", "marryme", "survivorsremorse"};
		printFile("/Users/alex/Desktop/tweetsovertime/lowvolume2.jpg", "Low Volume Shows 2", low2);
	}
	
	private void printFile(String filename, String title, String[] hashtags) {
		XYSeriesCollection collection = new XYSeriesCollection();
		JFreeChart chart = ChartFactory.createXYLineChart(
				title, "Day",
				"Number of Tweets", collection, PlotOrientation.VERTICAL, true,
				true, false);
		XYItemRenderer renderer = chart.getXYPlot().getRenderer();
		renderer.setSeriesPaint(0, Color.BLUE);
		renderer.setSeriesPaint(1, Color.RED);
		renderer.setSeriesPaint(2, Color.BLACK);
		renderer.setSeriesPaint(3, Color.GREEN);
		renderer.setSeriesPaint(4, Color.MAGENTA);
		renderer.setSeriesPaint(5, Color.CYAN);
		
		for (int i = 0; i < hashtags.length; i++) {
			Paint p = renderer.getSeriesPaint(i);
			LegendItem item = new LegendItem(hashtags[i], p);
		}
		chart.getLegend().setPosition(RectangleEdge.TOP);
		chart.setBackgroundPaint(Color.WHITE);
		ValueAxis vals = chart.getXYPlot().getDomainAxis();
		for (String h: hashtags) {
			collection.addSeries(this.timeSeries.get(h));
		}
		try {
			File out = new File(filename);
			ChartUtilities.saveChartAsJPEG(out, chart, 1200, 600);
		} catch (IOException e) {
			System.err.println("Problem occurred creating chart.");
		}
	}

	private List<Date> queueToList(Queue<Date> queue) {
		List<Date> list = new ArrayList<>();
		while (!queue.isEmpty()) {
			list.add(queue.poll());
		}
		return list;
	}

	private static String stringify(Date d) {
		return "" + d.getMonth() + "/" + d.getDay() + "/" + (d.getYear());
	}

	private static Date datify(String string) {
		String[] split = string.split("/");
		return new Date2(Integer.parseInt(split[2]), convert.get(split[0]) - 1,
				Integer.parseInt(split[1]), string);
	}

	private static class Date2 extends Date {

		private String string;

		public Date2(int year, int month, int day, String string) {
			super(year, month, day);
			this.string = string;
		}

		public String toString() {
			return string;
		}
	}

	public static void main(String[] args) throws IOException {
		(new TweetsPerDay("/Users/alex/Desktop/results2",
				"/Users/alex/Desktop/output2")).execute();
	}
}
