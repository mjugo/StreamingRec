package tudo.streamingrec.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.math3.exception.DimensionMismatchException;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import tudo.streamingrec.StreamingRec;
import tudo.streamingrec.data.Event;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.data.splitting.DataSplitter;
import tudo.streamingrec.evaluation.metrics.HypothesisTestableMetric;
import tudo.streamingrec.evaluation.metrics.Metric;

/**
 * A class for utility methods related to data loading and other stuff
 * 
 * @author Mozhgan
 *
 */
public class Util {

	/**
	 * Combines the list of metrics that is given as a parameter into a set of statistical test results.
	 * These results are then converted into a set of csv tables (one CSV table per metric).
	 * @param metrics -
	 * @param smirnov  -
	 * @return a csv table of pairwise statistical test results in the form of p-values
	 */
	public static String executeStatisticalTests(List<HypothesisTestableMetric> metrics, boolean smirnov) {
		//intialize the output format
		DecimalFormat df = new DecimalFormat("0.0000000");
		df.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));
		//extract the algorithm names from all metric objects
		List<String> algos = metrics.stream().map(Metric::getAlgorithm).distinct().collect(Collectors.toList());
		//initialize the output string builder
		StringBuilder sb = new StringBuilder();
		//some output string constants
		final String SEP = ";";
		final String LSEP = "\r\n";
		//map each metric name to a list of metric results objects
		Map<String, List<HypothesisTestableMetric>> metricsByType = metrics.stream()
				.collect(Collectors.groupingBy(HypothesisTestableMetric::getName));
		//iterate over all metrics
		for (Entry<String, List<HypothesisTestableMetric>> entry : metricsByType.entrySet()) {
			//header output
			sb.append(entry.getKey());
			sb.append(LSEP);
			//get a list of results for the metric per algorithm
			Map<String, HypothesisTestableMetric> metricsByAlgo = entry.getValue().stream()
					.collect(Collectors.toMap(HypothesisTestableMetric::getAlgorithm, Function.identity()));
			sb.append(SEP);// one line free
			//print a row of algorithms
			for (String a : algos) {
				sb.append(a);
				sb.append(SEP);
			}
			sb.append(LSEP);
			//iterate over all algorithm combinations
			for (String a1 : algos) {
				sb.append(a1);
				sb.append(SEP);
				for (String a2 : algos) {
					if(!a1.equals(a2)){
						try {
							//calculate the actual t-statistic
							if(!smirnov){
								sb.append(df.format(metricsByAlgo.get(a1).getTTestPValue(metricsByAlgo.get(a2))));
							}else{
								sb.append(df.format(metricsByAlgo.get(a1).getSmirnoffPValue(metricsByAlgo.get(a2))));
							}
						} catch (DimensionMismatchException ex) {
							//this should not happen. it's paired t-test, 
							//so every algorithms' result list needs to have the same length
							ex.printStackTrace();
							System.err.println(metricsByAlgo.get(a1).getAlgorithm());
							System.err.println(metricsByAlgo.get(a1).getName());
							System.err.println(metricsByAlgo.get(a2).getAlgorithm());
							System.err.println(metricsByAlgo.get(a2).getName());
						}
					}
					sb.append(SEP);
				}
				sb.append(LSEP);
			}
			sb.append(LSEP);
			sb.append(LSEP);
		}
		return sb.toString();
	}

	/**
	 * Write a list of click transactions to a file in the standard format
	 * @param transactions -
	 * @param fileName -
	 * @throws IOException -
	 */
	public static void writeTransactions(Collection<Transaction> transactions, String fileName) throws IOException {
		File foutRR = new File(fileName);
		FileOutputStream fosRR = new FileOutputStream(foutRR);
		BufferedWriter bwRR = new BufferedWriter(new OutputStreamWriter(fosRR));
		bwRR.write("ItemID,UserId,TimeStamp");
		bwRR.newLine();
		for (Transaction transaction : transactions) {
			bwRR.write(transaction.toString());
			bwRR.newLine();
		}
		bwRR.close();
	}

	/**
	 * Write a list of items to a file in the standard format
	 * @param items -
	 * @param fileName -
	 * @throws IOException -
	 */
	public static void writeItems(Collection<Item> items, String fileName) throws IOException {
		File foutIU = new File(fileName);
		FileOutputStream fosIU = new FileOutputStream(foutIU);
		BufferedWriter bwIU = new BufferedWriter(new OutputStreamWriter(fosIU));
		bwIU.write("Publisher,CreatedAt,ItemID,URL,Title");
		bwIU.newLine();
		for (Item item : items) {
			bwIU.write(item.toString());
			bwIU.newLine();
		}
		bwIU.close();
	}

	/**
	 * After sorting item and events in one list based on the event time in the
	 * {@link DataSplitter}, we split them up again here into two separate lists
	 * of items and transactions for convenience.
	 * 
	 * @param inputEvents -
	 * @param outputItems -
	 * @param outputTransactions -
	 */
	public static void extractEventTypes(List<Event> inputEvents, List<Item> outputItems,
			List<Transaction> outputTransactions) {
		for (Event event : inputEvents) {
			if (event instanceof Item) {
				outputItems.add((Item) event);
			} else {
				outputTransactions.add((Transaction) event);
			}
		}
	}

	/**
	 * Sorts the entries of a map based on their values
	 * 
	 * @param <K> - 
	 * @param <V> - 
	 * @param map -
	 * @param ascending -
	 * @return the sorted map
	 */
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, final boolean ascending) {
		List<Map.Entry<K, V>> list = new ObjectArrayList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				if (ascending) {
					return (o1.getValue()).compareTo(o2.getValue());
				} else {
					return (o2.getValue()).compareTo(o1.getValue());
				}

			}
		});

		Map<K, V> result = new Object2ObjectLinkedOpenHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	/**
	 * Sort the entries of a map based on their values and return the only the keys as a list
	 * 
	 * @param <K> - 
	 * @param <V> - 
	 * @param map -
	 * @param ascending -
	 * @param output -
	 * @return a list of the key, sorted based on their values
	 */
	public static <K, V extends Comparable<? super V>> List<K> sortByValueAndGetKeys(Map<K, V> map,
			final boolean ascending, List<K> output) {
		List<Map.Entry<K, V>> list = new ObjectArrayList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				if (ascending) {
					return (o1.getValue()).compareTo(o2.getValue());
				} else {
					return (o2.getValue()).compareTo(o1.getValue());
				}

			}
		});

		for (Map.Entry<K, V> entry : list) {
			output.add(entry.getKey());
		}
		return output;
	}
	
	/**
	 * Prints the ETA in ms into a human readable string.
	 * 
	 * @param millis -
	 * @return a pretty-printed string that represents the ETA
	 */
	public static String printETA(long millis) {
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
		StringBuilder sb = new StringBuilder(64);
		sb.append(String.format("%02d", hours));
		sb.append(" h ");
		sb.append(String.format("%02d", minutes));
		sb.append(" m ");
		sb.append(String.format("%02d", seconds));
		sb.append(" s ");
		return sb.toString();
	}

	/**
	 * Sorts the entries of a map based on their values
	 * 
	 * @param <K> - 
	 * @param <V> - 
	 * @param map -
	 * @param ascending -
	 * @return the sorted map
	 */
	public static <K, V extends Comparable<? super V>> List<K> sortByValueAndGetKeys(Map<K, V> map,
			final boolean ascending) {
		return sortByValueAndGetKeys(map, ascending, new ObjectArrayList<K>());
	}

	// output redirection constants
	private static boolean alreadyRedirected = false;
	private static final String folder = "output";
	private static final String fileNamePrefixStdOut = "stdout_";
	private static final String fileNamePrefixStdErr = "stderr_";
	private static final String fileNamePostfix = ".txt";

	/**
	 * redirects the output of the console to a file while also displaying it in
	 * the console at the same time
	 */
	public static void redirectConsole() {
		if (alreadyRedirected) {
			return;
		}
		try {
			new File(folder +"/"+StreamingRec.startTime).mkdirs();
			FileOutputStream fos = new FileOutputStream(folder +"/"+StreamingRec.startTime + "/" + fileNamePrefixStdOut + StreamingRec.startTime + fileNamePostfix);
			FileOutputStream fosErr = new FileOutputStream(folder +"/"+StreamingRec.startTime + "/" + fileNamePrefixStdErr + StreamingRec.startTime + fileNamePostfix);
			// we will want to print in standard "System.out" and in "file"
			TeeOutputStream myOut = new TeeOutputStream(System.out, fos);
			PrintStream ps = new PrintStream(myOut);
			System.setOut(ps);

			myOut = new TeeOutputStream(System.err, fosErr);
			ps = new PrintStream(myOut);
			System.setErr(ps);
			alreadyRedirected = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
