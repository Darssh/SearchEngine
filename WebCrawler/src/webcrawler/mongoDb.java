package webcrawler;

import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.jsoup.nodes.*;

public class mongoDb {
				
	public static void saveDocument(Map<String, Object> documentMap ){
		
		try{ 
			@SuppressWarnings("resource")
			MongoClient mongo = new MongoClient("localhost",27017);	
			MongoDatabase dB = mongo.getDatabase("search");
			MongoCollection<Document> collection=dB.getCollection("webdata");
		
			collection.insertOne(new Document(documentMap));
		
		
		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
}