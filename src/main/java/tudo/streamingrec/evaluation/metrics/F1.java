package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.evaluation.metrics.PrecisionOrRecall.Type;

/**
 * The F1 metric built on an overall level by taking 
 * the Precsion mean and the Recall mean 
 * and calculating the F1 based on these two values.
 * Another variant that builts the F1 for every eval result,
 * which is thus t-testable, is {@link MeanF1}.
 * @author MJ
 *
 */
public class F1 extends Metric{
	private static final long serialVersionUID = 3769912743935871425L;
	//the delegation objects
	private PrecisionOrRecall precision = null;
	private PrecisionOrRecall recall = null;	

	@Override
	public void evaluate(Transaction transaction, LongArrayList recommendations, LongOpenHashSet userTransactions) {
		if(precision==null){
			//if the delegation objects are not yet created, create them
			precision = new PrecisionOrRecall();
			precision.setType(Type.Precision);
			precision.setK(k);
			
			recall = new PrecisionOrRecall();
			recall.setType(Type.Recall);
			recall.setK(k);
		}
		
		//delegate the work to Precision and Recall instances
		precision.evaluate(transaction, recommendations, userTransactions);
		recall.evaluate(transaction, recommendations, userTransactions);
	}

	@Override
	public double getResults() {
		//retrieve the mean results of Precision and Recall
		double p = precision.getResults();
		double r = recall.getResults();
		
		//stolen from Recommender 101
		//build the F1 value as the harmonic mean of the mean Precision and Recall
		double f1;
		if (p+r == 0) {
			f1 = 0;
		}
		else {
			f1 = 2*(p*r)/(p+r);
		}
		
		//return the metric value
		return f1;
	}
}
