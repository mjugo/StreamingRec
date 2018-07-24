package tudo.streamingrec.algorithms;

import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;

/**
 * Post-filters the results of another algorithm based on the categories
 * of the currently clicked item or the items from the current user session.
 * @author MK, MJ
 *
 */
public class CategoryPostFiltering extends Algorithm{
	//the algorithm whose results should be post-filtered (set via JSON configuration)
	protected Algorithm mainStrategy;
	//should we fill append the items that were filtered from the result list again at the end of the list?
	private boolean fallback = false;
	//should we consider the categories of all items from the current session or just the last clicked item?
	private boolean considerSession = false;
	//a map of categories for each item
	private Long2IntOpenHashMap categoryMap = new Long2IntOpenHashMap();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//iterate over every click, retrieve the category for each item, and store it in the category map
		for (ClickData c : clickData) {
			categoryMap.put(c.click.item.id, c.click.item.category);
		}
		//let the underlying algorithm train
		mainStrategy.train(items, clickData);
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//filter the items that match with the current click's category
		//first, retrieve the recommendation results from the underlying algorithm
		LongArrayList allRec = mainStrategy.recommendInternal(clickData);
		
		//if we want to consider all categories from the items of the current session,
		//create a hashset with all these item categories
		Set<Integer> setOfCategoryInSession = null;
		if (considerSession) {
			setOfCategoryInSession = new IntOpenHashSet();
			for (Transaction t : clickData.session) {
				setOfCategoryInSession.add(t.item.category);
			}
		}
		
		//create lists of filtered items and retained items
		LongArrayList allRecInSameCategory = new LongArrayList();
		LongArrayList allRecNotSameCategory = new LongArrayList();
		//iterate over the recommendations
		for (int j = 0; j < allRec.size(); j++) {
			long id = allRec.getLong(j);
			int category = categoryMap.get(allRec.getLong(j));
			//check the category of the i-th item in the recommendation list
			if ((!considerSession && category == clickData.click.item.category)
					|| (considerSession && setOfCategoryInSession.contains(category))) {
				allRecInSameCategory.add(id);
			} else if (fallback) {
				//if we have a fallback, add the filtered item to the fallback list
				allRecNotSameCategory.add(id);
			}
		}
		//merge the filtered list with the fallback list (empty in case fallback==false)
		allRecInSameCategory.addAll(allRecNotSameCategory);
		//return the filtered list
		return allRecInSameCategory;
	}
	
	/**
	 * Set the underlying strategy (done via JSON config)
	 * @param algorithm -
	 */
	public void setMainStrategy(Algorithm algorithm){
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
	 * should we consider the categories of all items from the current session or just the last clicked item?
	 * @param considerSession -
	 */
	public void setConsiderSession(boolean considerSession) {
		this.considerSession = considerSession;
	}
}
