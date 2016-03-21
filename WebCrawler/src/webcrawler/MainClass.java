package webcrawler;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainClass {
	
	public static void main(String[] args) throws IOException {
		int depth = Integer.parseInt(args[0]);
		String url = args[1];
		boolean extractionMode = false;
		if(args.length == 3){
			extractionMode = true;
		}
		
		Queue<CrawlURL> pagesToCrawl = new ConcurrentLinkedQueue<>();
		pagesToCrawl.add(new CrawlURL(url, depth, extractionMode));

		
        Crawler T1 = new Crawler( url, depth, extractionMode, pagesToCrawl);
        
        Crawler T2 = new Crawler(url, depth, extractionMode, pagesToCrawl);

        ExecutorService exectors = Executors.newFixedThreadPool(2);
        exectors.execute(T1);
        try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        exectors.execute(T2);
        exectors.shutdown();
	}
}
