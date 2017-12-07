package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;

/**
 * Mean Reciprocal Rank Metric
 * @author MJ
 *
 */
public class MRR extends HypothesisTestableMetric{
	private static final long serialVersionUID = 2820604512185295616L;
	// result storage
	private DoubleArrayList results = new DoubleArrayList();

	@Override
	public void evaluate(Transaction transaction, LongArrayList recommendations,
			LongOpenHashSet userTransactions) {
		//if there is no ground truth, there is nothing to evaluate
		if (userTransactions == null || userTransactions.isEmpty()) {
			return;
		}
		
		// if the algorithm does not return any recommendations, count it as 0
		if (recommendations.isEmpty()) {
			results.add(0);
			return;
		}

		// calculate the MRR
		// if the algorithm retrieves less than k recommendations, we calculate
		// the real k value for this case
		int realK = Math.min(k, recommendations.size());
		// iterate over relevant items and calculate recall rank
		for (Long itemID : userTransactions) {
			for (int i = 0; i < realK; i++) {
				if (itemID == recommendations.getLong(i)) {
					results.add(1d/(i+1));
					return;
				}
			}
		}
		//nothing found -> count as zero
		results.add(0);
	}

	@Override
	public double getResults() {
		//return the average result
		return getAvg(results);
	}

	@Override
	public DoubleArrayList getDetailedResults() {
		//return the detailed results for t-tests
		return results;
	}
}
