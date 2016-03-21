package webcrawler;



import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.bson.Document;



import org.json.simple.JSONObject;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Extractor { 
	
	private static final String folderPath = "/Users/Darsh/Desktop/images";

	public void extract(org.jsoup.nodes.Document document,Document doc){
		try {
			MongoClient mongo = new MongoClient("localhost",27017);  
			MongoDatabase dB = mongo.getDatabase("search");
			  
			Map<String, Object> data = new HashMap<String, Object>();
			Map<String, Object> data1 = new HashMap<String, Object>();

			BodyContentHandler handler = new BodyContentHandler(-1);
			Metadata metadata = new Metadata();
			
			Elements img = document.getElementsByTag("img");
			JSONObject imageData = new JSONObject();	
			for (Element el : img) {
				String src = el.absUrl("src");
				imageData = getImages(el);
			}

			
			//Parser will take filetype not doc itself so convert into bytes
				
			InputStream inputstream = new ByteArrayInputStream(doc.get("content").toString().getBytes());
			ParseContext pcontext = new ParseContext();
				
			//Html parser 
			HtmlParser htmlparser = new HtmlParser();
			htmlparser.parse(inputstream, handler, metadata,pcontext);
				 
		    String cont = handler.toString();
		    cont = cont.replace("\n", "").replace("\r", "");
			cont = cont.replaceAll("\\s+", " ");
		  
			data.put("URL", doc.get("url"));
			data.put("Content", cont);
			data.put("Metadata",data1);
			data.put("headers", doc.get("headers"));
			data.put("imageData", imageData.toString());	
			String[] metadataNames = metadata.names();
				
			for(String name : metadataNames) {
			    data1.put(name ,metadata.get(name));
			}

			MongoCollection<Document> collection=dB.getCollection("extracteddata");    
			collection.insertOne(new Document(data));
			System.out.println("Inserted");
		}catch(Exception e){
			         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}	
	}
	
    private static JSONObject getImages(Element el) throws IOException {
    	String src = el.absUrl("src");
    	String folder = null;
    	JSONObject imageMeta = new JSONObject();
    	
    	int indexname = src.lastIndexOf("/");
    	if (indexname == src.length()) {
    		src = src.substring(1, indexname);
    	}
    	
    	indexname = src.lastIndexOf("/");
    	int foldername = src.substring(0, indexname).hashCode();
    	String name = src.substring(indexname, src.length());
    
    	URL url = new URL(src);
    	InputStream in = url.openStream();
    	File file = new File(folderPath+ "/" + foldername);
    	file.mkdirs();
    	
    	
    	OutputStream out = new BufferedOutputStream(new FileOutputStream(file + "/"+ name));
    	for (int b; (b = in.read()) != -1;) {
    		out.write(b);
    	}
    	out.close();
    	in.close();
    	
        File img = new File(file + "/"+ name);
        ImageInputStream iis = ImageIO.createImageInputStream(img);
        Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
      
        if (readers.hasNext()) {
            ImageReader read = readers.next();
            read.setInput(iis, true);

            IIOMetadata metadata = read.getImageMetadata(0);

            String[] names = metadata.getMetadataFormatNames();
            
            
            int length = names.length;
            for (int i = 0; i < length; i++) {
                JSONObject imgMeta = new JSONObject();
                imgMeta.put("Format name", metadata.getAsTree(names[i]).getNodeName());
                imgMeta.put("Value", metadata.getAsTree(names[i]).getNodeValue());
                imageMeta.put("imageData", imgMeta);
            }
            
            imageMeta.put("height", read.getHeight(0));
            imageMeta.put("metadata", read.getImageMetadata(0));
            imageMeta.put("format", read.getFormatName());
            imageMeta.put("width", read.getWidth(0));
            
        }
        return imageMeta;

    }

}