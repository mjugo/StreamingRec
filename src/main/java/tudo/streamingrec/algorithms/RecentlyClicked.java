package tudo.streamingrec.algorithms;

import java.util.List;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * The recommendations of this algorithm are all news articles that were clicked by users,
 * ordered by the time of the last user click.
 * 
 * @author Mozhgan
 *
 */
public class RecentlyClicked extends Algorithm{
	//a linked list of article ids ordered by their last user click time
	public LongLinkedOpenHashSet clickedItems = new LongLinkedOpenHashSet();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//whenever a click occurs, remove the id of the article that has been clicked
		//from the result list and push it to the front 
		for (ClickData c : clickData) {
			clickedItems.addAndMoveToFirst(c.click.item.id);
		}
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//return an array list copy of the ordered article ids
		return new LongArrayList(clickedItems);
	}

}
