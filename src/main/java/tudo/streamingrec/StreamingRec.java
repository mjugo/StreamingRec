package tudo.streamingrec;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tudo.streamingrec.AlgorithmWrapper.WorkPackage;
import tudo.streamingrec.AlgorithmWrapper.WorkPackageArticle;
import tudo.streamingrec.AlgorithmWrapper.WorkPackageClick;
import tudo.streamingrec.algorithms.Algorithm;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Event;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.RawData;
import tudo.streamingrec.data.SplitData;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.data.loading.FilteredDataReader;
import tudo.streamingrec.data.session.SessionExtractor;
import tudo.streamingrec.data.splitting.DataSplitter;
import tudo.streamingrec.evaluation.metrics.HypothesisTestableMetric;
import tudo.streamingrec.evaluation.metrics.Metric;
import tudo.streamingrec.util.Util;

/**
 * Main entry point for the framework
 * 
 * @author MK, MJ
 *
 */
@Command(name = "StreamingRec", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "A java framework for news recommendation algorithm evaluation. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class StreamingRec {
	//the item input file
	@Option(names = {"-i", "--items"}, paramLabel="<FILE>", description = "Path to the item input file in CSV format") 
	private static String INPUT_FILENAME_ITEMS = "data/Items.csv";
	//the click input file
	@Option(names = {"-c", "--clicks"}, paramLabel="<FILE>", description = "Path to the clicks input file in CSV format") 
	private static String INPUT_FILENAME_CLICKS =  "data/Clicks.csv";
	//are we using the "old" format, i.e., the inefficient format optimized only for plista?	
	@Option(names = {"-f", "--old-format"}, description = "Uses the old click file format") 
	private static boolean OLD_FILE_FORMAT = false; 
	//should we deduplicate the input files?
	@Option(names = {"-d", "--deduplicate"}, description = "Deduplicates the data")
	private static boolean DEDUPLICATE = false;
	//the path to the metric json config file
	@Option(names = {"-m", "--metrics-config"}, paramLabel="<FILE>", description = "Path to the metrics json config file")
	private static String METRICS_FILE_NAME = "config/metrics-config.json";
	//the path to the algorithm json config file
	@Option(names = {"-a", "--algorithm-config"}, paramLabel="<FILE>", description = "Path to the algorithm json config file")
	private static String ALGORITHM_FILE_NAME = "config/algorithm-config-simple.json";
	// if the next parameter is true -> consider an inactivity time threshold to
	// separate sessions; otherwise: built a session from elements of the same day
	@Option(names = {"-s", "--session-inactivity-threshold"}, description = "Uses a time-based session inactivity threshold to separate user sessions. Otherwise, all user events of one day represent one sessions.")
	private static boolean SESSION_INACTIVITY_THRESHOLD = false;
	//the time for the sessions inactivity threshold
	@Option(names = {"-t", "--session-time-threshold"}, paramLabel="<VALUE>", description = "The idle time threshold for separating two user sessions in milliseconds.")
	private static long SESSION_TIME_THRESHOLD = 1000 * 60 * 20;
	// if this parameter is set to greater than 0, sessions with this number or
	// less clicks will be removed from the data set
	@Option(names = {"-l", "--session-length-filter"}, paramLabel="<VALUE>", description = "If set to N, sessions with N or less clicks will be removed from the dataset. If set to 0, nothing will be filtered.")
	private static int SESSION_LENGTH_FILTER = 1;
	// if you don't want verbose stats, turn the next variable to false
	@Option(names = {"-o", "--output-stats"}, description = "Outputs more detailed stats. Might take more time.")
	private static boolean OUTPUT_STATS = false;
	//where to split the data into training and test
	@Option(names = {"-p", "--split-threshold"}, paramLabel="<VALUE>", description = "Split threshold for splitting the dataset into training and test set")
	private static double SPLIT_THRESHOLD = 0.7;
	//the number of threads to use
	@Option(names = {"-n", "--thread-count"}, paramLabel="<VALUE>", description = "Number of threads to use. Less threads result in less CPU usage but also less RAM usage.")
	private static int THREAD_COUNT = Runtime.getRuntime().availableProcessors()-1;
	
	//the global start time used for output writing to the same folder
	public static String startTime;	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new StreamingRec(), args);
		if (helpRequested) {
		   CommandLine.usage(new StreamingRec(), System.out);
		   return;
		}
		//if deduplication is deactivated -> write a warning in red
		if (!DEDUPLICATE) {
			System.err.println("Warning! Deduplication disabled.");
		}
		//save the global start time
		Calendar instance = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
		dateFormat.setTimeZone(instance.getTimeZone());
		startTime = dateFormat.format(instance.getTime());	
		//redirect the console output to a file
		Util.redirectConsole();
		//set the sessions extractor's session split thresholds
		SessionExtractor.setSessionInactivityThreshold(SESSION_INACTIVITY_THRESHOLD);
		SessionExtractor.setThresholdInMS(SESSION_TIME_THRESHOLD);

		//create a human readable session threshold for the output
		String timeThreshold = ISOPeriodFormat.standard().print(new Period(SESSION_TIME_THRESHOLD)).replace("PT", "");
		// output the parameters 
		System.out.println(
				"Input files: \"" + INPUT_FILENAME_ITEMS + "\" & \"" + INPUT_FILENAME_CLICKS + "\"");
		System.out.println("Config files: \"" + ALGORITHM_FILE_NAME + "\" & \"" + METRICS_FILE_NAME + "\"");
		if (SESSION_INACTIVITY_THRESHOLD) {
			System.out.println("session Time Thresholds: \"" + timeThreshold + "\"");
		} else {
			System.out.println("sessions based on cut off at midnight");
		}
		System.out.println("Session length filter: " + SESSION_LENGTH_FILTER);
		System.out.println("Split threshold: " + SPLIT_THRESHOLD);
		System.out.println();

		// load algorithms so that configuration errors appear before input file loading
		List<Algorithm> tmpAlgorithms = Config.loadAlgorithms(ALGORITHM_FILE_NAME);

		// output names of algorithms and check redundant names
		Set<String> names = new ObjectOpenHashSet<>();
		System.out.println("Tested algorithms:");
		for (Algorithm algorithm : tmpAlgorithms) {
			if (!names.add(algorithm.getName())) {
				System.err.println(
						"Duplicate name for algorithm: \"" + algorithm.getName() + "\". Please check config file.");
				System.err.println("Terminating");
				return;
			}
			System.out.println(algorithm.getName());
		}
		System.out.println();

		// read the data
		FilteredDataReader reader = new FilteredDataReader();
		RawData data = reader.readFilteredData(INPUT_FILENAME_ITEMS, INPUT_FILENAME_CLICKS, OUTPUT_STATS,
				DEDUPLICATE, OLD_FILE_FORMAT);

		//if a minimum session length is set, filter short sessions
		if (SESSION_LENGTH_FILTER > 0) {
			System.out.println();
			System.out.println("Filtering sessions shorter than or equal to " + SESSION_LENGTH_FILTER + " ...");
			// filter data based on too short sessions
			Set<Transaction> transactionsToRemove = new ObjectOpenHashSet<>();
			// create a session storage to filter too short sessions
			SessionExtractor filterExtractor = new SessionExtractor();
			for (Transaction t : data.transactions) {
				filterExtractor.addClick(t);
			}
			//check the length of sessions and remember the transactions that belong to short sessions
			for (List<List<Transaction>> list : filterExtractor.getSessionMap().values()) {
				for (List<Transaction> list2 : list) {
					if (list2.size() <= SESSION_LENGTH_FILTER) {
						for (Transaction transaction : list2) {
							transactionsToRemove.add(transaction);
						}
					}
				}
			}
			//keep all transactions except the ones that belong to short sessions in a new transaction list
			List<Transaction> filteredTransactions = new ObjectArrayList<>();
			for (Transaction t : data.transactions) {
				if (!transactionsToRemove.contains(t)) {
					filteredTransactions.add(t);
				}
			}
			//print some removal stats
			System.out.println("Removed "
					+ (((data.transactions.size() - filteredTransactions.size()) * 100) / data.transactions.size())
					+ "%");
			data.transactions = filteredTransactions;
			System.out.println("Number of transactions: " + data.transactions.size());
		}

		//in case stats are wanted, print extensive stats
		if (OUTPUT_STATS) {
			// overall stats
			Long2IntOpenHashMap clicksPerUser = new Long2IntOpenHashMap();
			Long2IntOpenHashMap clicksPerItem = new Long2IntOpenHashMap();
			// session stats			
			SessionExtractor sessionExtractorForStats = new SessionExtractor();
			for (Transaction t : data.transactions) {
				clicksPerItem.addTo(t.item.id, 1);
				clicksPerUser.addTo(t.userId, 1);
				sessionExtractorForStats.addClick(t);
			}
			
			//clicks per items and user
			DescriptiveStatistics clicksPerUserStats = new DescriptiveStatistics();
			DescriptiveStatistics clicksPerItemStats = new DescriptiveStatistics();
			for (Integer val : clicksPerUser.values()) {
				clicksPerUserStats.addValue(val);
			}
			System.out.println("Clicks per user: " + clicksPerUserStats);
			for (Integer val : clicksPerItem.values()) {
				clicksPerItemStats.addValue(val);
			}
			System.out.println("Clicks per item: " + clicksPerItemStats);

			// some statistics about session length
			DescriptiveStatistics stats = new DescriptiveStatistics();
			DescriptiveStatistics statsPerUser = new DescriptiveStatistics();
			DescriptiveStatistics lengthStats = new DescriptiveStatistics();
			Collection<List<List<Transaction>>> allSessions = sessionExtractorForStats.getSessionMap().values();
			for (List<List<Transaction>> list : allSessions) {
				for (List<Transaction> list2 : list) {
					stats.addValue(list2.size());
					if (list2.size() > 1) {
						long duration = list2.get(list2.size() - 1).timestamp.getTime()
								- list2.get(0).timestamp.getTime();
						lengthStats.addValue(duration);
					}
				}
				statsPerUser.addValue(list.size());

			}
			System.out.println("Clicks per session: " + stats);
			System.out.println("Sessions per user: " + statsPerUser);
			System.out.println("Length of session in MS: " + lengthStats);
		}

		// split the data
		DataSplitter splitter = new DataSplitter();
		splitter.setSplitMethodNumberOfEvents(); // split based on the number of
													// events, not the time
		splitter.setSplitThreshold(SPLIT_THRESHOLD); 
		// split after N% of the events.
		// Everything after that goes into the test set
		SplitData splitData = splitter.splitData(data);
		data = null;

		// re-extract the events based on type (item or transaction) for later convenience
		List<Item> trainingItems = new ObjectArrayList<Item>();
		List<Transaction> trainingTransactions = new ObjectArrayList<Transaction>();
		Util.extractEventTypes(splitData.trainingData, trainingItems, trainingTransactions);

		// create algorithms
		Map<String, Algorithm> algorithmsWithName = new Object2ObjectLinkedOpenHashMap<String, Algorithm>();
		for (Algorithm alg : tmpAlgorithms) {
			algorithmsWithName.put(alg.getName(), alg);
		}

		// create list of metrics
		Map<String, List<Metric>> metrics = new Object2ObjectLinkedOpenHashMap<String, List<Metric>>();
		Map<Metric, String> metricsByAlgorithm = new Object2ObjectLinkedOpenHashMap<Metric, String>();
		Map<String, List<Metric>> metricsByName = new Object2ObjectLinkedOpenHashMap<String, List<Metric>>();
		// for each algorithm, create a list of metrics
		for (String algorithmName : algorithmsWithName.keySet()) {
			List<Metric> metricsList = Config.loadMetrics(METRICS_FILE_NAME);
			for (Metric m : metricsList) {
				m.setAlgorithm(algorithmName);
				//add the metrics to maps for better retrieval
				addMetricToMaps(m, algorithmName, m.getName(), metrics, metricsByAlgorithm, metricsByName);
			}
		}

		// create main session extractor and user history helper
		SessionExtractor sessionExtractor = new SessionExtractor();
		Map<Long, List<Transaction>> userHistory = new Long2ObjectOpenHashMap<>();
		List<ClickData> trainingWorkPackages = new ObjectArrayList<>();
		for (Transaction t : trainingTransactions) {
			trainingWorkPackages
					.add(((WorkPackageClick) getWorkPackage(t, sessionExtractor, null, userHistory)).clickData);
		}
		//save some RAM
		trainingTransactions = null;

		// create a thread pool executor to limit the number of concurrent
		// threads to avoid thrashing
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

		// extract all sessions of users for evaluation phase
		// create map of next transactions
		List<Transaction> testTransactions = new ObjectArrayList<Transaction>();
		Util.extractEventTypes(splitData.testData, new ObjectArrayList<Item>(), testTransactions);
		SessionExtractor sessionExtractorforEvaluation = new SessionExtractor();
		for (Transaction t : testTransactions) {
			sessionExtractorforEvaluation.addClick(t);
		}
		//save some RAM
		testTransactions = null;

		// test phase
		int nextPercentage = 0;
		List<WorkPackage> testWorkPackages = new ObjectArrayList<>();
		for (int i = 0; i < splitData.testData.size(); i++) {
			// log the progress
			int percentage = (int) ((1d * (i + 1) / splitData.testData.size()) * 10);
			if (percentage == nextPercentage) {
				System.out.println("Creating work packages. Progress: " + percentage * 10 + "%");
				nextPercentage++;
			}
			// extract the current event
			Event currentEvent = splitData.testData.get(i);
			//create a work package (with click, session, ground truth, etc.)
			//and add it to the list of test packages
			testWorkPackages
					.add(getWorkPackage(currentEvent, sessionExtractor, sessionExtractorforEvaluation, userHistory));
		}

		// create threaded wrappers
		AlgorithmWrapper.nbOfAlgorithms = algorithmsWithName.size();
		for (Entry<String, Algorithm> alg : algorithmsWithName.entrySet()) {
			AlgorithmWrapper wrapper = new AlgorithmWrapper(alg.getValue(), metrics.get(alg.getKey()), trainingItems,
					trainingWorkPackages, testWorkPackages);
			//execute right away
			executor.execute(wrapper);
		}
		//save some RAM
		algorithmsWithName = null;
		trainingItems = null;
		trainingWorkPackages = null;
		testWorkPackages = null;

		//wait for all algorithms to finish (note: AlgorithmWrapper class does some tmp output)
		executor.shutdown();
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
			// wait for threads to finish
		}

		// output parameters again for convenience
		System.out.println();
		System.out.println(
				"Input files: \"" + INPUT_FILENAME_ITEMS + "\" & \"" + INPUT_FILENAME_CLICKS + "\"");
		System.out.println("Config files: \"" + ALGORITHM_FILE_NAME + "\" & \"" + METRICS_FILE_NAME + "\"");
		if (SESSION_INACTIVITY_THRESHOLD) {
			System.out.println("session Time Thresholds: \"" + timeThreshold + "\"");
		} else {
			System.out.println("sessions based on cut off at midnight");
		}
		System.out.println("Session length filter: " + SESSION_LENGTH_FILTER);
		System.out.println("Split threshold: " + SPLIT_THRESHOLD);
		System.out.println();

		// print evaluation results extracted from metric classes
		DecimalFormat df = new DecimalFormat("0.0000000");
		df.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));

		//print the actual results
		for (Entry<String, List<Metric>> ml : metricsByName.entrySet()) {
			System.out.println(ml.getKey());
			for (Metric m : metricsByName.get(ml.getKey())) {
				System.out.println(
						StringUtils.rightPad(metricsByAlgorithm.get(m), 70, ' ') + "\t" + df.format(m.getResults()));
			}
		}
		
		//create and print statistical significance tests
		List<HypothesisTestableMetric> statMetrics = new ArrayList<>();
		for (Entry<String, List<Metric>> ml : metricsByName.entrySet()) {
			if(ml.getValue().iterator().next() instanceof HypothesisTestableMetric){
				for (Metric metric : ml.getValue()) {
					statMetrics.add((HypothesisTestableMetric) metric);
				}				
			}
		}
		if(OUTPUT_STATS){
			System.out.println();
			System.out.println("---- STATISTICAL RESULTS ----");
			System.out.println();
			System.out.println(Util.executeStatisticalTests(statMetrics));
		}		
	}

	/**
	 * create a {@link WorkPackage} from an event (click or new item)
	 * @param event -
	 * @param sessionExtractor -
	 * @param sessionExtractorforEvaluation -
	 * @param userHistory -
	 * @return the work package
	 */
	private static WorkPackage getWorkPackage(Event event, SessionExtractor sessionExtractor,
			SessionExtractor sessionExtractorforEvaluation, Map<Long, List<Transaction>> userHistory) {
		if (event instanceof Item) {
			//in case of an item, just wrap it
			WorkPackageArticle wpA = new WorkPackageArticle();
			wpA.articleEvent = (Item) event;
			return wpA;
		} else {
			//in case of a click, wrap the click
			//+ find the appropriate session, all previous clicks in other sessions of this user, 
			//and the the ground truth related to this click 
			WorkPackageClick wpC = new WorkPackageClick();
			Transaction currentTransaction = (Transaction) event;
			wpC.clickData = new ClickData();
			wpC.clickData.click = currentTransaction;
			// extract the current user session (for removal of
			// duplicate/unnecessary recommendations later)
			List<Transaction> currenctUserSession = Collections
					.unmodifiableList(new ObjectArrayList<>(sessionExtractor.addClick(currentTransaction)));
			wpC.clickData.session = currenctUserSession;
			//extract the user history
			List<Transaction> history = userHistory.get(currentTransaction.userId);
			if (history == null) {
				history = new ObjectArrayList<>();
				userHistory.put(currentTransaction.userId, history);
			}
			history.add(currentTransaction);//add the current click to the user history
			//make it an unmodifiable list
			wpC.clickData.wholeUserHistory = Collections.unmodifiableList(new ObjectArrayList<>(history));
			if (sessionExtractorforEvaluation != null) {
				// from the session, extract the list of unique item ids
				LongOpenHashSet uniqueItemIDSoFar = new LongOpenHashSet();
				for (Transaction t : currenctUserSession) {
					uniqueItemIDSoFar.add(t.item.id);
				}
				//check with the user history to remove unwanted "reminders"
				List<Transaction> wholeCurrentUserSession = sessionExtractorforEvaluation
						.getSession(currentTransaction);
				//extact the ground truth
				LongOpenHashSet groundTruth = new LongOpenHashSet();
				for (Transaction t : wholeCurrentUserSession) {
					groundTruth.add(t.item.id);
				}
				// all transactions from the list that have already happened +
				// transactions for items that have already been clicked (no
				// reminders)
				groundTruth.removeAll(uniqueItemIDSoFar);
				wpC.groundTruth = groundTruth;
			}
			return wpC;
		}
	}

	/**
	 * Add each metrics to some maps so that they can be found later and printed nicely.
	 * 
	 * @param metric -
	 * @param algorithmName -
	 * @param metricName -
	 * @param metricsByAlgorithm -
	 * @param metricsToAlgorithm -
	 * @param metricsByName -
	 */
	private static void addMetricToMaps(Metric metric, String algorithmName, String metricName,
			Map<String, List<Metric>> metricsByAlgorithm, Map<Metric, String> metricsToAlgorithm,
			Map<String, List<Metric>> metricsByName) {
		metricsToAlgorithm.put(metric, algorithmName);
		List<Metric> list = metricsByName.get(metricName);
		if (list == null) {
			list = new ObjectArrayList<Metric>();
			metricsByName.put(metricName, list);
		}
		list.add(metric);
		list = metricsByAlgorithm.get(algorithmName);
		if (list == null) {
			list = new ObjectArrayList<Metric>();
			metricsByAlgorithm.put(algorithmName, list);
		}
		list.add(metric);
	}
	
	/**
	 * the item input file
	 * @return the item input file
	 */
	static String getInputFilenameItems() {
		return INPUT_FILENAME_ITEMS;
	}

	/**
	 * the click input file
	 * @return the click input file
	 */
	static String getInputFilenameClicks() {
		return INPUT_FILENAME_CLICKS;
	}

	/**
	 * the path to the metric json config file
	 * @return the path to the metric json config file
	 */
	static String getMetricsFileName() {
		return METRICS_FILE_NAME;
	}

	/**
	 * the path the the algorithm json config file
	 * @return the path the the algorithm json config file
	 */
	static String getAlgorithmFileName() {
		return ALGORITHM_FILE_NAME;
	}

	/**
	 * if the next parameter is true -> consider an inactivity time threshold to
	 * separate sessions; otherwise: built a session from elements of the same day
	 * @return the session Inactivity Threshold
	 */
	static boolean isSessionInactivityThreshold() {
		return SESSION_INACTIVITY_THRESHOLD;
	}

	/**
	 * the time for the sessions inactivity threshold
	 * @return the time for the sessions inactivity threshold
	 */
	static long getSessionTimeThreshold() {
		return SESSION_TIME_THRESHOLD;
	}

	/**
	 * if this parameter is set to greater than 0, sessions with this number or
	 * less clicks will be removed from the data set
	 * @return the session Length Filter
	 */
	static int getSessionLengthFilter() {
		return SESSION_LENGTH_FILTER;
	}

	/**
	 * where to split the data into training and test
	 * @return the split Threshold
	 */
	static double getSplitThreshold() {
		return SPLIT_THRESHOLD;
	}

	/**
	 * the global start time used for output writing to the same folder
	 * @return the global start Time
	 */
	static String getStartTime() {
		return startTime;
	}
}
