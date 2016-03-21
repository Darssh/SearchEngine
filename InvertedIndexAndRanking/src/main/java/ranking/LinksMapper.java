package ranking;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinksMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	DecimalFormat twoDForm = new DecimalFormat("0.000000000");
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println(value.toString());
		String line = value.toString();
		String[] docs = line.split("\t");
		if(docs.length == 3){
			String[] docsAndCount = docs[0].split("\t");
			String[] numberDocs = docs[2].split("\t");
		
			for(String n : numberDocs){
				context.write(new Text(n),  new Text(docs[2]+ "@" + twoDForm.format(Double.parseDouble(docsAndCount[1])/numberDocs.length).toString()));
			}
		}
	}

}