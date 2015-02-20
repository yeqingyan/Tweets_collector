package com.jandrom.twitter_collection.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class PolarityToArray {

	public PolarityToArray() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(
				new FileReader(new File("/Users/alex/Desktop/polarity.csv")));
		String line = reader.readLine(); //Throw away first line.
		while ((line = reader.readLine()) != null) {
		    String[] split = line.split(",");
		    int totalTweets = (int) Double.parseDouble(split[0]);
		    double withPolarity = Double.parseDouble(split[1]);
		    double positive = Double.parseDouble(split[2]);
		    double negative = Double.parseDouble(split[3]);
		    String hashtag = split[5];
		    System.out.printf("[ ' ', %f,\t %f,\t'%s',\t%d\t],\n", (withPolarity/totalTweets)*100, (positive/withPolarity)*100, hashtag, totalTweets);
		}
	}

}
