package tudo.streamingrec.data;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Meta data about an item
 * 
 * @author Mozhgan
 *
 */
public class Item implements Event {
	//the ID of the item
	public long id;
	//the publisher of the news article
	public int publisher;
	//the data when this article was first published
	public Date createdAt;
	//the URL of the article
	public String url;
	//the plain-text title of the article
	public String title;
	//the category of the item 
	public int category;
	//the plain-text content of the item
	public String text;
	//a set of keywords
	public Object2IntOpenHashMap<String> keywords;

	public Item() {
	}

	/**
	 * Instantiates an item from a line in a csv file
	 * 
	 * @param csvLine -
	 * @throws ParseException -
	 */
	public Item(String csvLine) throws ParseException {
		String[] split = csvLine.split(",");
		publisher = Integer.parseInt(split[0]);
		createdAt = Constants.DATE_FORMAT.parse(split[1]);
		id = Long.parseLong(split[2]);
		url = split[3];
		title = split[4];
		if (split.length > 5) {
			category = Integer.parseInt(split[5]);
		}
		if (split.length > 6) {
			text = split[6];
		}
		if (split.length > 7) {
			keywords = new Object2IntOpenHashMap<>();
			String[] keywordArr = split[7].split(Pattern.quote("#"));
			for (String string : keywordArr) {
				String[] split2 = string.split(Pattern.quote("-"));
				keywords.addTo(split2[0], Integer.parseInt(split2[1]));
			}
		}
	}

	/**
	 * Returns the item as a CSV line
	 */
	@Override
	public String toString() {
		String keywords = "";
		//in case there are keywords -> serialize them 
		if(this.keywords!=null){
			StringBuilder sb = new StringBuilder();
			ObjectIterator<Entry<String>> fastIterator = this.keywords.object2IntEntrySet().fastIterator();
			while(fastIterator.hasNext()){
				Entry<String> next = fastIterator.next();
				sb.append(next.getKey().replace("-","").replace("#", ""));
				sb.append("-");
				sb.append(next.getIntValue());
				if(fastIterator.hasNext()){
					sb.append("#");
				}
			}
			keywords = sb.toString();
		}
		//write CSV string
		return publisher + Constants.CSV_SEPARATOR + Constants.DATE_FORMAT.format(createdAt) + Constants.CSV_SEPARATOR
				+ id + Constants.CSV_SEPARATOR + url + Constants.CSV_SEPARATOR + title + Constants.CSV_SEPARATOR
				+ category + Constants.CSV_SEPARATOR + text + Constants.CSV_SEPARATOR + keywords;
	}
	
	/**
	 * Return the publication time of the item as a sorting criterion
	 */
	public Date getEventTime() {
		return createdAt;
	}

	/**
	 * Items are considered equal if they have the same ID
	 */
	@Override
	public boolean equals(Object item) {
		if (this.id == ((Item) item).id) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * See {@link #equals(Object)}
	 */
	@Override
	public int hashCode() {
		return Long.valueOf(id).hashCode();
	}
}
