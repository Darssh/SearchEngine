package indexing;

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
	MongoClient mongo;
	
	public mongoDb(){
		mongo = new MongoClient("localhost",27017);
	}
	
	public void saveDocument(Map<String, Object> documentMap ){
		
		try{ 
			@SuppressWarnings("resource")
			
			MongoDatabase dB = mongo.getDatabase("search");
			MongoCollection<Document> collection=dB.getCollection("webdata");
		
			collection.insertOne(new Document(documentMap));
			mongo.close();
		
		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
}