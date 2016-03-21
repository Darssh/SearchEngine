package webcrawler;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.Block;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class data_dump {
	
	public static void main(String[] args) {
		
		MongoClient mongo = new MongoClient("localhost",27017);  
		MongoDatabase dB = mongo.getDatabase("search");
		
		MongoCollection<Document> collection = dB.getCollection("extracteddata");
        System.out.println("Collection mycol selected successfully");
			
        FindIterable<Document> cursor = collection.find();
  	  
        cursor.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
            	try {
            		
            		Gson gson = new GsonBuilder().setPrettyPrinting().create();
            		JsonParser jp = new JsonParser();
            		JsonElement je = jp.parse(document.toJson());
            		String prettyJsonString = gson.toJson(je);
            		

                    System.out.println(prettyJsonString);
            		FileWriter file = new FileWriter("test.json", true);
            		file.write(prettyJsonString);
            		file.write(System.getProperty( "line.separator" ));
            		file.flush();
            		file.close();

            	} catch (IOException e) {
            		e.printStackTrace();
            	}

            }
        });
	}
}
