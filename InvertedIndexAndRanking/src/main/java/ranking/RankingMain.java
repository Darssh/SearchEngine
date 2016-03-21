package ranking;

import indexing.FirstMapper;
import indexing.FirstReducer;
import indexing.Indexer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.net.URLDecoder;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class RankingMain {
	
	
	public static void main(String[] args) throws Exception {
		htmlParserForLinks(args[0]);
//		long startTime = System.currentTimeMillis();
//		RankingMain ranking = new RankingMain();
//		Configuration conf = new Configuration();
//		ranking.firstJob(conf, args);
//		long endTime   = System.currentTimeMillis();
//		long totalTime = endTime - startTime;
//		System.out.println(totalTime);
	    System.exit(0);
	}
	
	public int firstJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf,"ranking");
		
		job.setJarByClass(this.getClass());
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[1] + "/links.txt"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/rankOutput"));
		
		job.setMapperClass(LinksMapper.class);
		//job.setCombinerClass(LinksReducer.class);
		job.setReducerClass(LinksReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void htmlParserForLinks(String filePath) throws IOException, NoSuchAlgorithmException{
		
		DecimalFormat twoDForm = new DecimalFormat("0.000000000");
		File dir = new File(filePath);
		Collection<File> directoryListing = FileUtils.listFiles(
				  dir, 
				  new RegexFileFilter("^(.*?)"), 
				  DirectoryFileFilter.DIRECTORY
				);
		
		int count = 0;
		ArrayList<String> paths = new ArrayList<String>();
		if (directoryListing != null) {
			   for (File child : directoryListing) { 
					if(child.getName().equals(".DS_Store") || child.isHidden()){
						continue;
					} else {
						paths.add(child.getName().toString());
					}
					count++;
			   }
		}
		
		if (directoryListing != null) {
		   for (File child : directoryListing) {
			  try {
					if(child.getName().equals(".DS_Store") || child.isHidden()){
						continue;
					}
				Document doc = Jsoup.parse(child, null);
	
				Elements links = doc.select("a[href]");
				Collection<String> coll = new HashSet<String>();
				for(Element link: links){
					coll.add(link.attr("href").toString());
				}
				

				File targetFile = new File("/Users/Darsh/Documents/cs454-Search Engine/links.txt");
				File parent = targetFile.getParentFile();
				if(!parent.exists() && !parent.mkdirs()){
				    throw new IllegalStateException("Couldn't create dir: " + parent);
				}
				if (!targetFile.exists()) {
					targetFile.createNewFile();
				}
				FileWriter fw = new FileWriter(targetFile.getAbsoluteFile(), true);
				BufferedWriter bw = new BufferedWriter(fw);
				
				bw.write(child.getName().toString()+"\t" + twoDForm.format((double) 1/count) + "\t" );
				StringBuilder stringBuilder = new StringBuilder();
				
				for(String link : coll){
					if(paths.contains(link)){
						stringBuilder.append(link);
						stringBuilder.append(",");
					}
				}
				bw.write(stringBuilder.toString());
				bw.write("\n");
				bw.close();
				

			  } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			  }
		   }
		}
		
	}
}
