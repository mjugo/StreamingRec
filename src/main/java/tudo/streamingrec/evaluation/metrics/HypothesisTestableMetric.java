package tudo.streamingrec.evaluation.metrics;

import java.io.Serializable;

import org.apache.commons.math3.stat.inference.TTest;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

/**
 * A metric that can provide test results for every sample point
 * so that statistical test can be performed.
 * @author MJ
 *
 */
public abstract class HypothesisTestableMetric extends Metric implements Serializable{
	private static final long serialVersionUID = -3343868792390624219L;
	/**
	 * Return the detailed results of every single recommendation list evaluation,
	 * so that later on the null-hypothesis (that the means of two different algorithms
	 * are the same) can be tested.
	 * @return A list of all individual evaluation results
	 */
	public abstract DoubleArrayList getDetailedResults();
	
	/**
	 * Returns the result of a two-tailed paired t-test. 
	 * Since n should always be greater 30, normality can be assumed.
	 * @param otherAlgorithm -
	 * @return the p-value result of a paired t-test
	 */
	public double getTTestPValue(HypothesisTestableMetric otherAlgorithm){
		return new TTest().pairedTTest(getDetailedResults().toDoubleArray(), otherAlgorithm.getDetailedResults().toDoubleArray());
	}
}
