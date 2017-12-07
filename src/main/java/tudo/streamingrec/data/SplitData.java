package tudo.streamingrec.data;

import java.util.List;

import tudo.streamingrec.data.splitting.DataSplitter;

/**
 * Return value of {@link DataSplitter#splitData(RawData)}, i.e.,
 * all events (item updates and clicks) sorted by time and split into
 * two lists based on a training test split.
 * @author Mozhgan
 *
 */
public class SplitData {
	//item publications and click events for training in one list sorted by time
	public List<Event> trainingData;
	//item publications and click events for testing in one list sorted by time
	public List<Event> testData;
}
