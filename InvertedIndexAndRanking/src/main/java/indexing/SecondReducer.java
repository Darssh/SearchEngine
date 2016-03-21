package indexing;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		
		int totalCount = 0;
		Map<String, Integer> wordsWithCount = new HashMap<String, Integer>();
		for (Text value : values) {
			String[] wordsCount = value.toString().split("@");
			wordsWithCount.put(wordsCount[0], Integer.parseInt(wordsCount[1]));
			totalCount += Integer.parseInt(wordsCount[1]);
		}
		
		Iterator it = wordsWithCount.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        context.write(new Text(pair.getKey() + "@" + key.toString()), new Text(wordsWithCount.get(pair.getKey()) + "@"
                    + totalCount));
	        
	        it.remove();	   
	    }
		
	}

}