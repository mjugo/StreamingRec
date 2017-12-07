package tudo.streamingrec.data.loading;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.Frequency;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.RawData;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * Reads the filtered files for articles and transactions
 * and deduplicates them if necessary
 * 
 * @author Mozhgan
 *
 */
@Command(name = "FilteredDataReader", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "Reads the item and click input files and writes the deduplicated clicks to another click file. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class FilteredDataReader {
	//what follows are parameters for the main method (not needed for normal operation via runner)
	//the item input file
	@Option(names = {"-i", "--items"}, description = "Path to the item input file in CSV format")	
	private static String INPUT_FILENAME_ITEMS = "data/Items.csv";
	//the click input file
	@Option(names = {"-c", "--clicks"}, description = "Path to the clicks input file in CSV format") 
	private static String INPUT_FILENAME_CLICKS = "data/Clicks.csv";
	//out file
	@Option(names = {"-o", "--out-file"}, description = "Path to the output file") 	
	private static String OUTPUT_FILENAME = "data/Clicks_dedup.csv";
	//are we using the "old" format, i.e., the inefficient format optimized only for plista?	
	@Option(names = {"-f", "--old-format"}, description = "Uses the old click file format") 
	private static boolean OLD_FILE_FORMAT = true;
	//should we deduplicate the input files?
	@Option(names = {"-d", "--deduplicate"}, description = "Deduplicates the data")
	private static boolean DEDUPLICATE = true;
	// if you don't want verbose stats, turn the next variable to false
	@Option(names = {"-s", "--output-stats"}, description = "Outputs more detailed stats. Might take more time.")
	private static boolean OUTPUT_STATS = false;
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;
	
	/**
	 * Opens, optionally deduplicates, and saves the files again.
	 * Can be used to convert from the old to the new format.
	 * 
	 * @param args -
	 * @throws IOException  -
	 * @throws ParseException  -
	 */
	public static void main(String[] args) throws IOException, ParseException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new FilteredDataReader(), args);
		if (helpRequested) {
		   CommandLine.usage(new FilteredDataReader(), System.out);
		   return;
		}
		FilteredDataReader fdr = new FilteredDataReader();
		RawData readFilteredData = fdr.readFilteredData(INPUT_FILENAME_ITEMS, INPUT_FILENAME_CLICKS, OUTPUT_STATS, DEDUPLICATE, OLD_FILE_FORMAT);
		//write the filtered csv files
		// create a file to write Recommendation Request and also event notification
		Util.writeTransactions(readFilteredData.transactions, OUTPUT_FILENAME);
	}

	/**
	 * Reads an item and click event file into a RawData object.
	 * @param itemFile The item file to read from
	 * @param clickFile The click file to read from
	 * @param printStats Should we print stats (might take more longer)?
	 * @param deduplicate Should the data be deduplicated?
	 * @param oldFormat Is the item input data in the old format?
	 * @return A raw data object the represents the contents of the item and click event files
	 * @throws IOException -
	 * @throws ParseException -
	 */
	public RawData readFilteredData(String itemFile, String clickFile, boolean printStats, boolean deduplicate, boolean oldFormat)
			throws IOException, ParseException {
		// first, read the items file
		BufferedReader br = new BufferedReader(new FileReader(itemFile));
		String str = "";
		br.readLine();// discard header
		Map<Long, Item> items = new Long2ObjectOpenHashMap<Item>();
		while ((str = br.readLine()) != null) {
			Item i = new Item(str);//create an item object from each line
			items.put(i.id, i);//put the items in the map by their ID
		}
		br.close();
		System.out.println("Number of items: " + items.size());

		// second, read the transactions file
		List<Transaction> transactions = new ObjectArrayList<Transaction>();
		BufferedReader brT = new BufferedReader(new FileReader(clickFile));
		String strT = "";
		// discard header
		brT.readLine();
		//count some stats
		int duplicateCount = 0;
		int overAllCnt = 0;
		int lineCnt = 0;
		while ((strT = brT.readLine()) != null) {
			if(printStats && lineCnt++%1000000==0){
				//print progress regularly
				System.out.println(lineCnt/1000000 + "m lines");
			}
			//create a transaction object from the line 
			//the map of files is given as input to map the transaction to the right item
			Transaction transaction = new Transaction(strT, items, oldFormat);
			//removal of duplicate transactions
			boolean duplicate = false;
			if(deduplicate){
				for (int i = transactions.size() - 1; i >= 0
						&& transaction.timestamp.getTime() - transactions.get(i).timestamp.getTime() < 60000; i--) {
					// if the transaction is less then one minute older then the
					// current transaction
					// and the item and user are the same,
					// dont add the current transaction to the list
					if (transaction.item.id == transactions.get(i).item.id
							&& transaction.userId == transactions.get(i).userId) {
						duplicate = true;
						break;
					}
				}
			}
			//if the transaction is not a duplicate (or deduplication is shut of), add it to the list
			if (!duplicate) {
				transactions.add(transaction);
			} else {
				duplicateCount++;
			}
			overAllCnt++;
		}
		brT.close();
		if (printStats) {
			//print some overall stats
			System.out.println("Number of transaction (before dedup): " + overAllCnt);
			System.out.println("Removed because of dedup: " + duplicateCount);
		}

		System.out.println("Number of transaction: " + transactions.size());
		if(transactions.get(0).item==null){
			//safety check to make sure the right format was used
			System.err.println("Null item: did you use the wrong file format??");
		}

		//print some more elaborate stats
		if (printStats) {
			Set<Integer> categoriesI = new IntOpenHashSet();
			Frequency categoryFreq = new Frequency();
			//check categories
			for (Transaction t : transactions) {
				categoriesI.add(t.item.category);
				categoryFreq.addValue(t.item.category);
				if (t.item.category != t.item.category) {
					System.err.println("Incosistent categories in dataset");
					System.exit(0);
				}
			}
			//print category stats
			System.out.println();
			System.out.println("Number of categories (based on items): " + categoriesI.size());
			System.out.println("Frequencies of categories:");
			System.out.println(categoryFreq);
			System.out.println();
		}

		//create and return the RawData object
		RawData returnData = new RawData();
		returnData.items = items;
		returnData.transactions = transactions;
		return returnData;
	}
}
