package indexing;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	MongoClient mongo = new MongoClient("localhost",27017);
	MongoDatabase dB = mongo.getDatabase("search");
	MongoCollection<Document> collection=dB.getCollection("webdata");
	
	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		IndexMapper index = new IndexMapper();
		StringBuilder stringBuilder = new StringBuilder();
        
        JsonObject albums = new JsonObject();
        JsonArray datasets = new JsonArray();
        BasicDBList dbl = new BasicDBList();
        for(Text val : values){
        	
        	String[] sep = val.toString().split("@");

        	JsonObject dataset = new JsonObject();
        	//dataset.addProperty("totalDocContainWord", sep[1]);
        	
        	double tfIdf =  Double.parseDouble(sep[5].toString()) / index.maxTfIdf;
        	dbl.add(new BasicDBObject("docName" , sep[0].toString()).append("tdIdf", tfIdf));
        	//dataset.addProperty("totalDoc", sep[2]);
        	//dataset.addProperty("wordCountInDoc", sep[3]);
        	dataset.addProperty("totalWordInDoc", sep[4]);
        	dataset.addProperty("tfIdf", sep[5]);
        	
            datasets.add(dataset);
        }
        
		albums.add(key.toString(), datasets);

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		String prettyJsonString = gson.toJson(datasets);
		
		Document document = new Document();
		document.put("word", key.toString());
		document.put("docs", dbl);

		collection.insertOne(new Document(document));
//		FileWriter file = new FileWriter("/Users/Darsh/Documents/cs454-Search Engine/test.json", true);
//		file.write(prettyJsonString);
//		file.write(System.getProperty( "line.separator" ));
//		file.flush();
//		file.close();
        
        
//		for (Text value : values) {
//			
//			stringBuilder.append(value.toString());
//			
//			if (values.iterator().hasNext()) {
//				stringBuilder.append(" -> ");
//			}
//		}

		context.write(key, new Text(stringBuilder.toString()));
	}

}