package indexing;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jsoup.Jsoup;
import org.tartarus.snowball.ext.PorterStemmer;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

public class IndexMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	public static double maxTfIdf = 0;
	public static double minTfIdf = 1;
	
	public void map(LongWritable key,Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] docs = line.split(" | ");
		String[] wordDoc = docs[0].split("@");
		context.write(new Text(wordDoc[0]), new Text(wordDoc[1] + "@" + docs[2] ));
		
		if(Double.parseDouble(docs[2].split("@")[4]) > maxTfIdf){
			maxTfIdf = Double.parseDouble(docs[2].split("@")[4]);
		}
		if(Double.parseDouble(docs[2].split("@")[4]) < minTfIdf){
			minTfIdf = Double.parseDouble(docs[2].split("@")[4]);
		}
	}
}