package ranking;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LinksReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		double pagerank = 0;
		double DAMPING_FACTOR = 0.85;
		Text list = new Text();
		StringBuilder stringBuilder = new StringBuilder();
		for (Text val : values) {
			System.out.println(val.toString());
			if(isNumeric(val.toString())){
				pagerank += Double.parseDouble(val.toString());
			}	else {
				list = val;
			}
        }
		pagerank = 1 - DAMPING_FACTOR + ( DAMPING_FACTOR * pagerank );
//		System.out.println(key.toString() + "@" + String.valueOf(pagerank));
//		System.out.println(list);
		stringBuilder.append(key.toString());
		stringBuilder.append("@");
		stringBuilder.append(pagerank);
		context.write(new Text(stringBuilder.toString()), list);
	}

	public static boolean isNumeric(String str) {  
	  try {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe) {  
	    return false;  
	  }  
	  return true;  
	}
}