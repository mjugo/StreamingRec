package tudo.streamingrec.data;

import java.util.List;
import java.util.Map;

import tudo.streamingrec.data.loading.FilteredDataReader;

/**
 * Return value of {@link FilteredDataReader#readFilteredData(String, String, boolean, boolean,boolean)}, i.e.,
 * the contents of the item update and click files.
 * @author Mozhgan
 *
 */
public class RawData {
	//the item metadata mapped by item ID
	public Map<Long, Item> items;
	//all clicks in a list
	public List<Transaction> transactions;
}
