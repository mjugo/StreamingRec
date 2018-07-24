package tudo.streamingrec.algorithms;

import java.util.List;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * Post-filters the results of another algorithm.
 * Removes all items that have not been clicked at least once within the last N minutes/hours/days.
 * @author MK, MJ
 *
 */
public class RecentlyClickedPostFiltering extends Algorithm {
	//the time in which each item needs to receive at least once click, to not be filtered from the list
	private long filterTime =  3 * 60 * 60 * 1000;// default = 3 h
	//the underlying algorithm (set via JSON config)
	protected Algorithm mainStrategy;
	//should we fill append the items that were filtered from the result list again at the end of the list?
	private boolean fallback = false;
	
	//a map that saves the timestamp of the last click each item has received
	private Long2LongOpenHashMap itemClickTime = new Long2LongOpenHashMap();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//for each click update the last-clicked time of the item that has been clicked
		for (ClickData c : clickData) {
			itemClickTime.put(c.click.item.id, c.click.timestamp.getTime());
		}
		//let the underlying algorithm train
		mainStrategy.train(items, clickData);
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//filter out items that have not received at last one click in the last time frame
		//first, retrieve the recommendation results of the underlying algorithm
		LongArrayList rec = mainStrategy.recommendInternal(clickData);
		
		//create lists of filtered items and retained items
		LongArrayList filteredRec = new LongArrayList();
		LongArrayList filteredRecNotMatch = new LongArrayList();
		//iterate over the recommendation list of the underlying strategy
		for (int j = 0; j < rec.size(); j++) {
			long i = rec.getLong(j);
			//filter items whose last-clicked timestamp is too old
			if ((itemClickTime.containsKey(i)) && ((clickData.click.timestamp.getTime()-itemClickTime.get(i))<filterTime)) {
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
	 * the time in which each item needs to receive at least once click, to not be filtered from the list
	 * @param filterTime -
	 */
	public void setFilterTime(long filterTime) {
		this.filterTime = filterTime;
	}

	/**
	 * the underlying algorithm (set via JSON config)
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
}
