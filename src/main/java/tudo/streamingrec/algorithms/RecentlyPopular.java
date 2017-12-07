package tudo.streamingrec.algorithms;

import java.util.LinkedList;
import java.util.List;

import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * This algorithm calculates the number of clicks on each item 
 * during a specific time frame starting in the past and ending at the current instant 
 * (e.g. 1 hour ago till now, 1 day ago till now, ...).
 * The algorithm then recommends items ordered by click count (highest to lowest) in this time frame.
 * 
 * @author Mozhgan
 *
 */
public class RecentlyPopular extends MostPopular {
	//The time up until which clicks are counted for the "popularity"
	private int filterTime = 24*60*60*1000;//default = 1 day (millisecond)
	//a buffer of recent transactions so that click counts can be decreased when they "leave" the time window
	private List<ClickData> recentTransactions = new LinkedList<ClickData>();
	
	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		//train the basic "most popular" implementation
		super.trainInternal(items, clickData);
		//add the training click to the transaction buffer
		recentTransactions.addAll(clickData);		
		//filter all clicks from the transaction buffer that have left the time window (e.g. when they occurred more than 1h ago)
		while((recentTransactions.get(recentTransactions.size()-1).click.timestamp.getTime()-recentTransactions.get(0).click.timestamp.getTime())>filterTime){
			//if the transactions get old, remove them from the list and decrease the popularity count
			if(recentTransactions.get(0).click.item!=null){
				clickCounter.addTo(recentTransactions.get(0).click.item.id, -1);
			}
			recentTransactions.remove(0);
		}		
		//for the actual recommending: let the super implementation do the work
	}
	
	
	/**
	 * Sets the time up until which clicks are counted for the "popularity"
	 * @param filterTime -
	 */
	public void setFilterTime(int filterTime) {
		this.filterTime = filterTime;
	}
}
