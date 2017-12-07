package tudo.streamingrec.algorithms;

import java.util.List;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * Post-filters the results of another algorithm based on the publication time of the candidate items.
 * @author Michael
 *
 */
public class RecencyPostFiltering extends Algorithm {
	//the maximum time before the current (simulation) time for an item to be released to not be filtered from the result list
	private int filterTime = 24 * 60 * 60 * 1000;// default = 1 day
	//the underlying algorithm (set via JSON config)
	protected Algorithm mainStrategy;
	//should we fill append the items that were filtered from the result list again at the end of the list?
	private boolean fallback = false;

	//a map with publication times for each item (id)
	private Long2LongOpenHashMap timestampMap = new Long2LongOpenHashMap();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//for each click, extract the item's publication time and add it to the map
		for (ClickData c : clickData) {
			timestampMap.put(c.click.item.id, c.click.item.createdAt.getTime());
		}
		//let the underlying algorithm train
		mainStrategy.train(items, clickData);
	}

	@Override
	public LongArrayList recommend(ClickData clickData) {
		//filter out items that have been release too long ago
		//first, retrieve the recommendation results of the underlying algorithm
		LongArrayList rec = mainStrategy.recommend(clickData);
		
		//create lists of filtered items and retained items
		LongArrayList filteredRec = new LongArrayList();
		LongArrayList filteredRecNotMatch = new LongArrayList();
		//iterate over the recommendation list of the underlying strategy
		for (int j = 0; j < rec.size(); j++) {
			long i = rec.getLong(j);
			// filter item based on the difference between the current (simulation) time and
			// the time of publication
			if ((clickData.click.timestamp.getTime() - timestampMap.get(i)) <= filterTime
					&& (clickData.click.timestamp.getTime() - timestampMap.get(i)) > 0) {
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
	 * the maximum time before the current (simulation) time for an item to be 
	 * released to not be filtered from the result list in milliseconds.
	 * 
	 * @param filterTime -
	 */
	public void setFilterTime(int filterTime) {
		this.filterTime = filterTime;
	}

}
