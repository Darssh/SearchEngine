package indexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		
		String[] doc = value.toString().split(" | ");

        String[] wordDoc = doc[0].split("@");

        StringBuilder string = new StringBuilder();
        string.append(wordDoc[0]);
        string.append("@");
        string.append(doc[2]);
        context.write(new Text(wordDoc[1]), new Text(string.toString()));
	}
}