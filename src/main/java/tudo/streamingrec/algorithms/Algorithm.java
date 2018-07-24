package tudo.streamingrec.algorithms;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;

/**
 * The core algorithm interface
 * 
 * @author Mozhgan
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "algorithm")
public abstract class Algorithm {
	// The name of the algorithm (extracted from the JSON configuration)
	private String name;
	// The training interval (i.e. the time interval in terms of simulation time
	// for each training batch)
	private int trainingInterval;
	// If set to true, we consider whole user history as the current session;
	// this emulates personalization in some algorithms
	private boolean wholeUserHistory = false;

	// buffer for training data (only used in case of batch learning (training
	// interval == 0)
	private List<ClickData> trainBufferClick = new ObjectArrayList<>();
	private List<Item> trainBufferArticle = new ObjectArrayList<>();
	// remeber the last train time
	private long lastTrainTime = 0;

	/**
	 * This method is called by the evaluation framework once in the beginning
	 * with the main training data and after every click. Depending on the
	 * setting of the trainingInterval parameter, this method than immediately
	 * calls the algorithm implementation's {@link #trainInternal(List, List)}
	 * method or it stores the training data in a buffer and calls the training
	 * method of the algorithm after the appropriate time interval with the
	 * whole buffer as a batch.
	 * 
	 * @param items
	 *            New items
	 * @param transactions
	 *            New transactions
	 */
	public final void train(List<Item> items, List<ClickData> transactions) {
		// if the training interval is not set, we give the training data to the
		// algorithm immediately
		if (trainingInterval <= 0) {
			trainInternal(items, transactions);
			return;
		}
		// otherwise -> buffer the data
		trainBufferClick.addAll(transactions);
		trainBufferArticle.addAll(items);
		if (!trainBufferClick.isEmpty()) {
			// check the training interval (in terms of simulation time)
			if ((trainBufferClick.get(trainBufferClick.size() - 1).click.timestamp.getTime() - lastTrainTime) / 1000
					/ 60 >= trainingInterval) {
				// let's batch train
				trainInternal(trainBufferArticle, trainBufferClick);
				lastTrainTime = trainBufferClick.get(trainBufferClick.size() - 1).click.timestamp.getTime();
				// clear the buffer
				trainBufferArticle.clear();
				trainBufferClick.clear();
			}
		}
	}

	/**
	 * The internal training method that has to be overriden by every algorithm
	 * implemenation. Called once in the beginning with the main training data
	 * and after every training interval has elapsed. In case
	 * {@link Algorithm#trainingInterval} is set to 0, this method is called for
	 * every new click immediately.
	 * 
	 * @param items
	 *            The new items
	 * @param transactions
	 *            The new transactions
	 */
	protected abstract void trainInternal(List<Item> items, List<ClickData> transactions);

	/**
	 * Called when one recommendation list should be produced by the algorithm.
	 * The algorithm should not training the model in this method. Even if the
	 * algorithm is supposed to learn after every click, it should be done in
	 * the {@link #trainInternal(List, List)} method. If the training interval
	 * is set to 0, this method will be called after every click.
	 * 
	 * @param clickData
	 *            the data related to the current user click (the click itself,
	 *            the session, and all previous user clicks)
	 * @return A recommendation list
	 */
	public final LongArrayList recommend(ClickData clickData) {
		if (wholeUserHistory) {
			ClickData d = new ClickData();
			d.click = clickData.click;
			d.session = clickData.wholeUserHistory;
			d.wholeUserHistory = clickData.wholeUserHistory;
			clickData = d;
		}
		return recommendInternal(clickData);
	}

	protected abstract LongArrayList recommendInternal(ClickData clickData);

	/**
	 * The name of the algorithm (extracted from the JSON configuration)
	 * 
	 * @return the name of the algorithm
	 */
	public String getName() {
		return name;
	}

	/**
	 * The name of the algorithm (extracted from the JSON configuration)
	 * 
	 * @param name
	 *            -
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * The training interval (i.e. the time interval in terms of simulation time
	 * for each training batch). Extracted from the JSON configuration.
	 * 
	 * @param trainingInterval
	 *            the training interval in minutes
	 */
	public void setTrainingInterval(int trainingInterval) {
		this.trainingInterval = trainingInterval;
	}

	/**
	 * If set to true, we consider whole user history as the current session;
	 * this emulates personalization in some algorithms
	 * 
	 * @param wholeUserHistory -
	 */
	public void setWholeUserHistory(boolean wholeUserHistory) {
		this.wholeUserHistory = wholeUserHistory;
	}

}
