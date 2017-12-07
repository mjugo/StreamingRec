package tudo.streamingrec.evaluation.metrics;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import tudo.streamingrec.AlgorithmWrapper;
import tudo.streamingrec.data.Transaction;

/**
 * A special metric that stores the runtime results of of an algorithm.
 * Values are filled directly by the {@link AlgorithmWrapper} object.
 * @author MJ
 *
 */
public class Runtime extends Metric {
	private static final long serialVersionUID = -1149304700693682960L;
	private double runtime;
	private Type type;
	private Resolution resolution;

	@Override
	public void evaluate(Transaction transaction, LongArrayList recommendations, LongOpenHashSet userTransactions) {
		//do nothing here.
		//runtime values are not based on the recommendation list
		//but on the runtime determined by the AlgorithmWrapper class
	}

	/**
	 * Sets the runtime value of this algorithm.
	 * Done by the {@link AlgorithmWrapper} object.
	 * @param runtime -
	 */
	public void setRuntime(double runtime) {
		//depending on the resolution, the value is divided
		switch (resolution) {
			case Seconds:
				this.runtime = runtime / 1000;
				return;
			case Minutes:
				this.runtime = runtime / 1000 / 60;
				return;
			case Hours:
				this.runtime = runtime / 1000 / 60 / 60;
				return;
			default:
				this.runtime = runtime;
				return;
		}
	}

	@Override
	public double getResults() {
		//return the runtime
		return runtime;
	}

	/**
	 * The type of runtime (training, testing, or in-between training)
	 * @return The type of runtime
	 */
	public Type getType() {
		return type;
	}

	/**
	 * The type of runtime (training, testing, or in-between training).
	 * Set via JSON config.
	 * @param type -
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * The type of runtime (training, testing, or in-between training)
	 * @author MJ
	 *
	 */
	public static enum Type {
		Training, Testing, InBetweenTraining
	}

	/**
	 * The resolution of the runtime (Milliseconds, Seconds, Minutes, or Hours)
	 * @author MJ
	 *
	 */
	public static enum Resolution {
		Milliseconds, Seconds, Minutes, Hours
	}

	/**
	 * The resolution of the runtime (Milliseconds, Seconds, Minutes, or Hours).
	 * Set via JSON config.
	 * @param resolution the resolution to set
	 */
	void setResolution(Resolution resolution) {
		this.resolution = resolution;
	}

}
