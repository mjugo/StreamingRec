package tudo.streamingrec.algorithms;

import java.util.List;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;

/**
 * Post-filters the results of another algorithm based on the popularity of the candidate items.
 * @author MK, MJ
 *
 */
public class PopularityPostFiltering extends Algorithm {
	//the minimum click count each item must have to not be filtered from the recommendation list
	private int minClickCount = 100;// default = 100 clicks
	//the underlying algorithm (set via JSON config)
	protected Algorithm mainStrategy;
	//should we fill append the items that were filtered from the result list again at the end of the list?
	private boolean fallback = false;
	
	//a map with click counts for each item
	private Long2IntOpenHashMap itemClickCount = new Long2IntOpenHashMap();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//if new click arrived, update the click counts for each item
		for (ClickData c : clickData) {
			increaseClickCount(c.click);
		}
		//let the underlying strategy train
		mainStrategy.train(items, clickData);
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//filter out items with low overall click counts
		//first, retrieve the recommendation results of the underlying algorithm
		LongArrayList rec = mainStrategy.recommendInternal(clickData);
		
		//create lists of filtered items and retained items
		LongArrayList filteredRec = new LongArrayList();
		LongArrayList filteredRecNotMatch = new LongArrayList();
		//iterate over the recommendation list of the underlying strategy
		for (int j = 0; j < rec.size(); j++) {
			long i = rec.getLong(j);
			//filter items if they do not have enough clicks
			if ((itemClickCount.containsKey(i)) && (itemClickCount.get(i) >= minClickCount)) {
				filteredRec.add(i);
			} else if (fallback) {
				//if we have a fallback, add the filtered item to the fallback list
				filteredRecNotMatch.add(i);
			}
		}
		//merge the filtered list with the fallback list (empty in case fallback==false)
		filteredRec.addAll(filteredRecNotMatch);
		//return the filtered list
		return filteredRec;
	}

	/**
	 * the minimum click count each item must have to not be filtered from the recommendation list
	 * @param minClickCount -
	 */
	public void setMinClickCount(int minClickCount) {
		this.minClickCount = minClickCount;
	}

	/**
	 * Set the underlying strategy (done via JSON config)
	 * @param algorithm -
	 */
	public void setMainStrategy(Algorithm algorithm) {
		mainStrategy = algorithm;
	}

	/**
	 * should we fill append the items that were filtered from the result list again at the end of the list?
	 * @param fallback -
	 */
	public void setFallback(boolean fallback) {
		this.fallback = fallback;
	}

	/**
	 * increases the click count of an item that has just been clicked on by 1
	 * @param transaction -
	 */
	private void increaseClickCount(Transaction transaction) {
		if (itemClickCount.containsKey(transaction.item.id)) {
			itemClickCount.put(transaction.item.id, itemClickCount.get(transaction.item.id) + 1);
		} else {
			itemClickCount.put(transaction.item.id, 1);
		}
	}

}
