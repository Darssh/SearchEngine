package webcrawler;

public class CrawlURL {
	
	public String url;
	public int depth;
	public boolean extraction;
	
	public CrawlURL(String url, int depth, boolean extraction){
		this.url = url;
		this.depth = depth;
		this.extraction = extraction;
	}
}
