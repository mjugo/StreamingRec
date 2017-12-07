package tudo.streamingrec.algorithms;

import java.util.List;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.util.Util;

/**
 * An algorithm that recommends the most popular articles based on click count
 * regardless of the category.
 * 
 * @author Mozhgan
 *
 */
public class MostPopular extends Algorithm {
	// In this list we keep all articles and their click counts
	protected Long2IntOpenHashMap clickCounter = new Long2IntOpenHashMap();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		// iterate through all transactions and increase the click count for the
		// respective item
		for (ClickData c : clickData) {
			clickCounter.addTo(c.click.item.id, 1);
		}
	}
	
	public LongArrayList recommend(ClickData clickData) {
		//return the items sorted by their click count
		return (LongArrayList) Util.sortByValueAndGetKeys(clickCounter, false, new LongArrayList());
	}
}
