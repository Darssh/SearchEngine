package indexing;

import java.io.BufferedWriter;
import java.security.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.Jsoup;

public class Indexer extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(Indexer.class);

	public int run(String[] args) throws Exception {
		return 0;
	}

	public static void main(String[] args) throws Exception {
		htmlParser(args);
		long startTime = System.currentTimeMillis();
		Indexer indexer = new Indexer();
		Configuration conf = new Configuration();
		 indexer.firstJob(conf, args);
		 indexer.secondJob(conf, args);
		 indexer.thirdJob(conf, args);
		indexer.indexJob(conf, args);
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		System.exit(0);
	}

	public int firstJob(Configuration conf, String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "indexer");

		job.addCacheFile(new Path(args[2]).toUri());
		job.setJarByClass(this.getClass());

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", " | ");

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[1] + "/files"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output"));

		job.setMapperClass(FirstMapper.class);
		job.setCombinerClass(FirstReducer.class);
		job.setReducerClass(FirstReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int secondJob(Configuration conf, String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "indexer");

		job.setJarByClass(this.getClass());

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", " | ");

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[1] + "/output"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output1"));

		job.setMapperClass(SecondMapper.class);
		job.setReducerClass(SecondReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int thirdJob(Configuration conf, String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "indexer");

		job.setJarByClass(this.getClass());

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", " | ");

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[1] + "/output1"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output2"));

		job.setMapperClass(ThirdMapper.class);
		job.setReducerClass(ThirdReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

//		Path inputPath = new Path(args[0]);
//		FileSystem fs = inputPath.getFileSystem(conf);
//		FileStatus[] stat = fs.listStatus(inputPath);

		job.setJobName(String.valueOf(6044));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int indexJob(Configuration conf, String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "indexer");

		job.addCacheFile(new Path(args[2]).toUri());
		job.setJarByClass(this.getClass());

		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", " | ");

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[1] + "/output2"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/indexed"));

		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void htmlParser(String[] args) throws IOException,
			NoSuchAlgorithmException {

		File dir = new File(args[0]);
		Collection<File> directoryListing = FileUtils.listFiles(dir,
				new RegexFileFilter("^(.*?)"), DirectoryFileFilter.DIRECTORY);

		if (directoryListing != null) {
			for (File child : directoryListing) {
				try {

					Document doc = Jsoup.parse(child, null);
					String htmlDoc = Jsoup.parse(doc.toString()).text();

					File targetFile = new File(args[1] + "/files/" + child.getName());
					
					File parent = targetFile.getParentFile();
					if (!parent.exists() && !parent.mkdirs()) {
						throw new IllegalStateException("Couldn't create dir: "
								+ parent);
					}
					if (!targetFile.exists()) {
						targetFile.createNewFile();
					}
					FileWriter fw = new FileWriter(targetFile.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(htmlDoc);
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
}