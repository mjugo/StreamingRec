package tudo.streamingrec.data;

import java.util.Date;
import java.util.Map;

/**
 * A transactions within the dataset, i.e., a page visit triggered by a user.
 * 
 * @author Mozhgan
 *
 */
public class Transaction implements Event {
	//the article that was visited
	public Item item;
	//the user that visited the article
	public long userId;
	//the time when the article was visited
	public Date timestamp;

	public Transaction() {
	}

	/**
	 * Instantiates an transaction from a line in a csv file
	 * 
	 * @param csvLine the string that represents one line from the csv file
	 * @param items a map of items that have to be loaded from the item file beforehand
	 * @param oldFileFormat new or old (plista-oriented) file format?
	 */
	public Transaction(String csvLine, Map<Long, Item> items, boolean oldFileFormat) {
		String[] splitT = csvLine.split(",");
		item = items.get(Long.parseLong(splitT[oldFileFormat?2:0]));
		userId = Long.parseLong(splitT[oldFileFormat?3:1]);
		timestamp = new Date(Long.parseLong(splitT[oldFileFormat?4:2]));
	}

	/**
	 * Converts the transaction to a CSV string
	 */
	@Override
	public String toString() {
		String itemID;
		if (item == null) {
			itemID = "null";
		} else {
			itemID = String.valueOf(item.id);
		}
		return itemID + Constants.CSV_SEPARATOR + userId + Constants.CSV_SEPARATOR + timestamp.getTime();
	}

	/**
	 * Return the click's timestamp as a sorting criterion
	 */
	public Date getEventTime() {
		return timestamp;
	}
}
