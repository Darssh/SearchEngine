package webcrawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

public class Headers {
	
	JSONObject headers = new JSONObject();
	
	public JSONObject getHeaders(String url_address) throws MalformedURLException{
		try {
			
			URL url = new URL(url_address);
			URLConnection connection = url.openConnection();
			
			JSONObject obj = new JSONObject();
			headers.put("Last-Modified",  connection.getLastModified());
			headers.put("Content-Length", connection.getContentLength());
			headers.put("Date", connection.getHeaderField("Date"));
			headers.put("Content-Type", connection.getContentType());

		} catch (IOException e) {
			e.printStackTrace();
		}
	    return headers;
	}
}
