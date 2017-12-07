package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.evaluation.metrics.PrecisionOrRecall.Type;

/**
 * Instead of building the macro-average F1 score (as in {@link F1}), 
 * we build the harmonic mean of precision and recall at every test point
 * and then take the average of that.
 * @author MJ
 *
 */
public class MeanF1 extends HypothesisTestableMetric{
	private static final long serialVersionUID = -3321203802140966473L;
	//the delegation objects
	private transient PrecisionOrRecall precision = null;
	private transient PrecisionOrRecall recall = null;
	
	// result storage
	private DoubleArrayList results;
	
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
		//calculate the results and return the average
		calcResults();
		return getAvg(results);
	}
	
	/**
	 * calculates the results of the F1 metric based on the eval results 
	 * of Precision and Recall at every step
	 */
	private synchronized void calcResults(){
		if(results==null){
			//get the detailed results of precision and recall
			DoubleArrayList precisionRes = precision.getDetailedResults();
			DoubleArrayList recallRes = recall.getDetailedResults();
			
			//create the F1 result list
			results = new DoubleArrayList();
			//iterate over the P/R results
			for(int i = 0; i < precisionRes.size(); i++){
				//retreive each P and R value and build the harmonic mean
				double p = precisionRes.getDouble(i);
				double r = recallRes.getDouble(i);
				double f1;
				if (p+r == 0) {
					f1 = 0;
				}
				else {
					f1 = 2*(p*r)/(p+r);
				}
				//save the result value
				results.add(f1);
			}
		}
	}

	@Override
	public DoubleArrayList getDetailedResults() {
		//calculate and return the t-testable array of F1 values
		calcResults();
		return results;
	}
}
