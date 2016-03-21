package indexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
        String[] docs = value.toString().split(" | ");
        String[] words = docs[0].toString().split("@");
        String[] counts = docs[2].toString().split("@");
        context.write(new Text(words[0]), new Text(words[1] + "=" + counts[0] + "@"+ counts[1]));
    }
}