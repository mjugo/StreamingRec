package tudo.streamingrec.evaluation.metrics;

import java.io.Serializable;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.data.Transaction;

/**
 * The core metric interface
 * 
 * @author Mozhgan
 *
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.MINIMAL_CLASS, include=JsonTypeInfo.As.PROPERTY, property="metric")
public abstract class Metric implements Serializable{	
	private static final long serialVersionUID = -2889599452592100391L;
	private String name;
	private String algorithm;

	// evaluation parameters
	protected int k = 10;
		/**
	 * The method called by runner class
	 * 
	 * @param transaction
	 *            the current transaction
	 * @param recommendations
	 *            the list of recommendations which is a child of Item class
	 * @param userTransactions
	 *            list of transactions for the current user which shows user's
	 *            next clicks
	 */
	public abstract void evaluate(Transaction transaction, LongArrayList recommendations,
			LongOpenHashSet userTransactions);

	/**
	 * return result of evaluation
	 * 
	 * @return the result of the evaluation (e.g. mean F1, mean MRR, etc.)
	 */
	public abstract double getResults();

	/**
	 * The number of items to consider from the top of the recommendation list.
	 * 
	 * @param k -
	 */
	public void setK(int k) {
		this.k = k;
	}

	/**
	 * The name of the metric (set via JSON). E.g. Precision@10
	 * @return the name of the metric
	 */
	public String getName() {
		return name;
	}

	/**
	 * The name of the metric (set via JSON). E.g. Precision@10
	 * @param name -
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Averages the results of multiple scores
	 * @param results -
	 * @return the average of a list of evaluation scores
	 */
	protected double getAvg(DoubleArrayList results){
		SummaryStatistics avg = new SummaryStatistics();
		for (Double val : results) {
			avg.addValue(val);
		}
		return avg.getMean();
	}

	/**
	 * The name of the algorithm that this metric belongs to (for t-tests) 
	 * @return the name of the algorithm
	 */
	public String getAlgorithm() {
		return algorithm;
	}

	/**
	 * The name of the algorithm that this metric belongs to (for t-tests) 
	 * @param algorithm -
	 */
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
}
