package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;

/**
 * Counts the number of unique items recommended in the 
 * top-N list of the recommender across all lists
 * @author MJ
 *
 */
public class NbRecItems extends Metric{
	private static final long serialVersionUID = -4702891242458198872L;
	//a set of item ids to count the number of unique items
	LongOpenHashSet uniqueItems = new LongOpenHashSet();

	@Override
	public void evaluate(Transaction transaction, LongArrayList recommendations, LongOpenHashSet userTransactions) {
		//merge the recommendation into the list
		uniqueItems.addAll(recommendations.subList(0, Math.min(recommendations.size(), k)));
	}

	@Override
	public double getResults() {
		//return the unique number of recommended items
		return uniqueItems.size();
	}

}
