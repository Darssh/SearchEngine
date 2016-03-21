package indexing;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.json.JSONObject;

public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values,
			final Context context) throws IOException, InterruptedException {
		
        int sum = 0;
        for (IntWritable val : values) {
        	System.out.println(val);
            sum += val.get();
            
        }
        context.write(key, new IntWritable(sum));
	}

}