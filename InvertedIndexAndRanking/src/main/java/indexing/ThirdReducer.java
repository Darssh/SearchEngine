package indexing;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<Text, Text, Text, Text> {
	
	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private Text word = new Text();
	private Text count = new Text();
	
	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		
		int totalDocs = Integer.parseInt(context.getJobName());
		int numOfDocsForKey = 0;
		Map<String, String> wordAndCount = new HashMap<String, String>();

        for (Text val : values) {
            String[] docAndCount = val.toString().split("=");
            numOfDocsForKey++;
            wordAndCount.put(docAndCount[0], docAndCount[1]);
        }
        for (String document : wordAndCount.keySet()) {
            String[] wordFrequenceAndTotalWords = wordAndCount.get(document).split("@");
 
            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / Double.valueOf(wordFrequenceAndTotalWords[1]));

            double idf = (double) totalDocs / (double) numOfDocsForKey;

            double tfIdf = totalDocs == numOfDocsForKey ?
                    tf : tf * Math.log10(idf);
            
            word = new Text(key.toString().trim() + "@" + document);
            count = new Text(numOfDocsForKey + "@" + totalDocs + "@" + wordFrequenceAndTotalWords[0] + "@" + wordFrequenceAndTotalWords[1] + "@" + DF.format(tfIdf) );

            
            context.write(word, count);
        }

		
	}

}