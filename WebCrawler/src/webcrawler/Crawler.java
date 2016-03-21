package webcrawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.helper.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.*;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

public class Crawler implements Runnable{
	
	String url;
	int depth;
	boolean extraction;
	private Thread t;
	
//	public static void main(String[] args) throws IOException {
//		int depth = Integer.parseInt(args[0]);
//		String url = args[1];
//		boolean extractionMode = false;
//		if(args.length == 3){
//			extractionMode = true;
//		}
//		
//		Queue<CrawlURL> pagesToCrawl = new LinkedList<>();
//		pagesToCrawl.add(new CrawlURL(url, depth, extractionMode));
//		
//		Crawler crawl = new Crawler();
//    	long startTime = System.nanoTime();
//		crawl.depthSearch(pagesToCrawl);
//    	long endTime = System.nanoTime();
//
//    	long duration = (endTime - startTime);
//
//	}
	

	
	public Crawler(String url,int depth,boolean extraction){
		this.url = url;
		this.depth = depth;
		this.extraction = extraction;
	}
	
	Queue<CrawlURL> pagesToCrawl;
	public Crawler(String url,int depth,boolean extraction, Queue<CrawlURL> pages){
		this.url = url;
		this.depth = depth;
		this.extraction = extraction;;
		this.pagesToCrawl = pages;
	}
	
	public void run(){
		depthSearch(pagesToCrawl);
	}
	
	public void depthSearch(Queue pagesToCrawl)  {
		while(!pagesToCrawl.isEmpty()){
			CrawlURL currentUrl = (CrawlURL) pagesToCrawl.remove();
			Map<String, Object> webdata = new HashMap<String, Object>();
			
			Headers header = new Headers();
			try {
				webdata.put("headers", header.getHeaders(currentUrl.url));
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
			
			
			Document doc;
			try {
				doc = Jsoup.connect(currentUrl.url).get();
//				webdata.put("url", currentUrl.url);
//				webdata.put("content", doc.toString());
//				savetofile(currentUrl.url.split("/data/")[1], doc);
				//mongoDb mongo = new mongoDb();
				//mongo.saveDocument(webdata);
				
				File path = new File(
						"/Users/Darsh/Documents/cs454-Search Engine/paths.txt");
				
				FileWriter fw1 = new FileWriter(path.getAbsoluteFile(),
						true);
				BufferedWriter bw1 = new BufferedWriter(fw1);
				bw1.write(doc.title() + ".html");
				bw1.write(" -> ");
				bw1.write("/Users/Darsh/Documents/cs454-Search Engine/crawldata/" +currentUrl.url.split("/data/")[1]);
				bw1.write("\n");
				bw1.close();
				
				org.bson.Document document= new org.bson.Document();
				document.put("hash", currentUrl.url.split("/data/")[1]);
				document.put("docName", doc.title() + ".html");
				document.put("docPath", "/Users/Darsh/Documents/cs454-Search Engine/crawldata/" +currentUrl.url.split("/data/")[1]);
				try{	
					MongoClient mongo = new MongoClient("localhost",27017);
					MongoDatabase dB = mongo.getDatabase("search");
					MongoCollection<org.bson.Document> collection=dB.getCollection("path");
				
					collection.insertOne(document);
					mongo.close();
			    }catch(Exception e){
					System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				}
					
				
				if(currentUrl.extraction == true){
					Extractor ext = new Extractor();
					ext.extract(doc, new org.bson.Document(webdata));
				}
				
				if(currentUrl.depth != 0){
					Elements links = doc.select("a[href]");

					for (Element link : links){
						pagesToCrawl.add(new CrawlURL(link.attr("abs:href"), currentUrl.depth - 1, currentUrl.extraction));
						depthSearch(pagesToCrawl);
					}				
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public void savetofile(String title, org.jsoup.nodes.Document doc) throws IOException{
		File targetFile = new File("/Users/Darsh/Documents/cs454-Search Engine/crawldata/"+ title);
		File parent = targetFile.getParentFile();
		if(!parent.exists() && !parent.mkdirs()){
		    throw new IllegalStateException("Couldn't create dir: " + parent);
		}
		if (!targetFile.exists()) {
			targetFile.createNewFile();
		}
		FileWriter fw = new FileWriter(targetFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(doc.toString());
		bw.close();
	}
	
}
