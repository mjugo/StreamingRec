package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;

/**
 * Calculates the coverage of each recommender algorithm (how many items are
 * recommended each time up to a maximum of k).
 * 
 * @author Mozhgan
 *
 */
public class Coverage extends HypothesisTestableMetric{
	private static final long serialVersionUID = -5868960950896363409L;
	//result storage
	private DoubleArrayList results = new DoubleArrayList();

	@Override
	public void evaluate(Transaction transaction, LongArrayList recommendations,
			LongOpenHashSet userTransactions) {
		//just count either k, or the actual number of recommendation 
		//in case the algorithm did not provide k recommendations
		results.add(Math.min(recommendations.size(), k));
	}

	@Override
	public double getResults() {
		//build the average
		return getAvg(results) / k;
	}

	@Override
	public DoubleArrayList getDetailedResults() {
		//return the detailed results for t-testing
		return results;
	}

}
