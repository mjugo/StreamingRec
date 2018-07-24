package tudo.streamingrec.algorithms;

import java.util.Collections;
import java.util.List;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * The candidate set of this algorithm is all news in the data set 
 * and it selects news articles randomly from this set for recommendation.
 * 
 * @author Mozhgan
 *
 */
public class Random extends Algorithm{
	//An unsorted set of all item ids
	private LongOpenHashSet items = new LongOpenHashSet();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//if new items arrive, add their ids to the set of item ids
		for (Item item : items) {
			this.items.add(item.id);
		}
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//create a result list and copy the known item ids there
		LongArrayList recs = new LongArrayList(items);
		//shuffle the result list and return it
		Collections.shuffle(recs);
		return recs;
	}

}
