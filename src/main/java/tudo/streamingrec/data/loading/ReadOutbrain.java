package tudo.streamingrec.data.loading;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import org.apache.commons.lang3.StringUtils;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;

/**
 * Iterates over the 2b Outbrain dataset, extracts all events for one publisher, 
 * sorts it by time, and saves it to a file in this framework's default format.
 * @author MJ
 *
 */
@Command(name = "ReadOutbrain", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "Extract item meta data and click events from the offcial outbrain dataset "
			+ "and outputs them in this framwork's format. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class ReadOutbrain {

	//the main input folder
	@Option(names = {"-f", "--input-folder"}, paramLabel="<FOLDER>", description = "Path to the input folder with all input files") 
	private static String inputFolder = "outbrain/";
	//the name of the page views input file
	@Option(names = {"-c", "--clicks"}, paramLabel="<FILE>", description = "Path to the clicks (page views) input file in zipped CSV format relative to the folder path") 
	private static String inputClickFile = "page_views.csv.zip";
	//the name of the item input file
	@Option(names = {"-i", "--items"}, paramLabel="<FILE>", description = "Path to the item (documents meta) input file in zipped CSV format relative to the folder path") 
	private static String inputItemFile = "documents_meta.csv.zip";
	//the names of the files that should be used as keywords (e.g. topics, categories, and entities)
	@Option(names = {"-k", "--keywords"}, paramLabel="<FILES>", description = "Path to the keyword input file in zipped CSV format relative to the folder path") 
	private static String[] inputItemTopicsFiles = {"documents_topics.csv.zip", "documents_categories.csv.zip", "documents_entities.csv.zip"};
	//the name of the item output file
	@Option(names = {"-I", "--out-items"}, paramLabel="<FILE>", description = "Path to the item output file") 
	private static String outputFileNameItemUpdate  = "data/Items.csv";
	//the name of the click event file
	@Option(names = {"-C", "--out-clicks"}, paramLabel="<FILE>", description = "Path to the clicks output file") 
	private static String outputFileNameClicks = "data/Events.csv";
	//which publisher should be selected?
	@Option(names = {"-p", "--publisher"}, paramLabel="<VALUE>", description = "Optional: Which publisher should we select from the data?") 
	private static Integer publisherFilter = null;
	//only retrieve the first N events for this publisher?
	@Option(names = {"-s", "--stop-after"}, paramLabel="<VALUE>", description = "Optional: Only retrieve the first N events for this publisher?") 
	private static Integer stopAfter = null;
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;

	public static void main(String[] args) throws IOException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new ReadOutbrain(), args);
		if (helpRequested) {
		   CommandLine.usage(new ReadOutbrain(), System.out);
		   return;
		}
		//create a set for the items 
		Set<Item> iSet = new HashSet<>();
		//initialize a date format for reading timestamps
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'hh:mm:ss");
		if(!inputFolder.endsWith("/")){
			inputFolder += "/";
		}
		System.out.println("Reading items ...");
		//open the item zip file
		try (ZipFile zipFile = new ZipFile(inputFolder + inputItemFile);
				InputStream inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				Stream<String> stream = br.lines()) {
			//read items line by line
			stream.skip(1)//skip the header
				.map(l -> l.split(","))//split by comma
				.forEach(s -> {
				try {
					//parse the file entry
					Item i = new Item();
					i.id = Long.parseLong(s[0]);
					i.publisher = Integer.parseInt(s[2]);
					i.createdAt = dateFormat.parse(s[3]);
					if (publisherFilter == null || i.publisher == publisherFilter) {
						//add the item if publisher should not be filtered, or publisher matches
						iSet.add(i);
					}
				} catch (ParseException ex) {
					ex.printStackTrace();
				} catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
				}
			});
		}

		//add the items to a map by their item id
		Map<Long, Item> itemMap = iSet.stream().collect(Collectors.toMap(i -> i.id, Function.identity()));
		System.out.println(iSet.size() + " items");
		System.out.println("Reading keywords ...");
		//open the keyword files one after another
		for(String file : inputItemTopicsFiles){
			try (ZipFile zipFile = new ZipFile(inputFolder + file);
					InputStream inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
					Stream<String> stream = br.lines()) {
				stream.skip(1)//skip header
					.map(l -> l.split(",")) //split by comma
					.forEach(s -> {
					//parse the keyword entry
					long id = Long.parseLong(s[0]);
					if (itemMap.containsKey(id)) {
						Item item = itemMap.get(id);
						double conf = Double.parseDouble(s[2]);
						//create a new keyword map for this item if necessary
						if (item.keywords == null) {
							item.keywords = new Object2IntOpenHashMap<>();
						}
						//safety check
						if(item.keywords.containsKey(s[1])){
							System.err.println("Duplicate: " + s[1]);
						}
						//add keyword to keyword map
						item.keywords.put(s[1], (int) conf * 100 + 1);
					}
				});
			}	
		}
		
		System.out.println("Reading clicks ...");
		//create a user id map (user ids in the Outbrain dataset are some kind of string uuid)
		Object2LongOpenHashMap<String> userIdMap = new Object2LongOpenHashMap<>();
		AtomicLong counter = new AtomicLong();
		//create a line counter
		AtomicInteger lCount = new AtomicInteger();
		//every timestamp has to be added to this offset (see official Outbrain doc on Kaggle)
		final long timestampOffset = 1465876799998l;
		//create a list of transactions
		List<Transaction> tList = new ArrayList<>();
		//remeber the items that actually received at least one click (more necessary in case of "stopAfter")
		Set<Long> seenItems = new HashSet<Long>();
		//iterate over click file
		try (ZipFile zipFile = new ZipFile(inputFolder + inputClickFile);
				InputStream inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				Stream<String> stream = br.lines()) {
			stream.skip(1)//skip header
				.map(l -> l.split(","))//split by comma
				.allMatch(s -> {
				if (lCount.incrementAndGet() % 1000000 == 0) {
					//output progress regularly
					System.out.println(lCount.get() / 1000000 + "m clicks overall. " + tList.size() / 1000 + "k clicks for selected publisher.");
				}
				if (stopAfter != null && tList.size() > stopAfter) {
					//in case only N events should be read, stop when count is reached
					return false;
				}
				//create a transaction object
				Transaction t = new Transaction();
				//find the item that belongs to this event
				t.item = itemMap.get(Long.parseLong(s[1]));
				//if no item was found, we are not interested
				if (t.item != null) {
					//add the item's id to the list of clicked items
					seenItems.add(t.item.id);
					//if the user is unknown, generate a long id for them
					if (!userIdMap.containsKey(s[0])) {
						userIdMap.put(s[0], counter.incrementAndGet());
					}
					//parse the rest of the click data and add it to the list
					t.userId = userIdMap.getLong(s[0]);
					t.timestamp = new Date(Long.parseLong(s[2]) + timestampOffset);
					tList.add(t);
				}
				//keep on reading
				return true;
			});
		}
		//sort the list of clicks
		tList.sort(new Comparator<Transaction>() {
			@Override
			public int compare(Transaction o1, Transaction o2) {
				return o1.timestamp.compareTo(o2.timestamp);
			}
		});
		//output some statistics
		System.out.println(tList.get(0).timestamp.toString());
		System.out.println(tList.get(tList.size() - 1).timestamp.toString());
		System.out.println(tList.size() + " transactions");

		//output some statistics about clicks per publisher
		Map<Integer, List<Transaction>> clicksByPublisher = tList.stream()
				.collect(Collectors.groupingBy(t -> t.item.publisher));
		clicksByPublisher.entrySet().stream().sorted((e1, e2) -> e1.getValue().size() - e2.getValue().size())
				.forEach(e -> {
					System.out.println(StringUtils.rightPad("Publisher " + e.getKey(), 10) + " ---> "
							+ StringUtils.leftPad(e.getValue().size() + "", 10));
				});

		//add the items that actually received some clicks to a list and sort it
		List<Item> iList = iSet.stream().filter(i -> seenItems.contains(i.id)).collect(Collectors.toList());
		iList.sort(new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				return o1.createdAt.compareTo(o2.createdAt);
			}
		});
		//print number of items
		System.out.println(iList.size() + " items");
		
		//open the writer for the clicks file
		File foutRR = new File(outputFileNameClicks);
		FileOutputStream fosRR = new FileOutputStream(foutRR);
		BufferedWriter bwRR = new BufferedWriter(new OutputStreamWriter(fosRR));
		bwRR.write("Publisher,Category,ItemID,Cookie,Timestamp,keywords");
		bwRR.newLine();
		//open the writer for the items file
		File foutIU = new File(outputFileNameItemUpdate);
		FileOutputStream fosIU = new FileOutputStream(foutIU);
		BufferedWriter bwIU = new BufferedWriter(new OutputStreamWriter(fosIU));
		bwIU.write("Domain,CreatedAt,ItemID,URL,Title,category,text,keywords");
		bwIU.newLine();
		bwIU.flush();
		//write the click events to the output file
		for (Transaction t : tList) {
			String string = t.toString();
			bwRR.write(string);
			bwRR.newLine();
		}
		//write the items to the output file
		for (Item i : iList) {
			bwIU.write(i.toString());
			bwIU.newLine();
		}
		//close the output files
		bwRR.flush();
		bwRR.close();
		bwIU.flush();
		bwIU.close();
	}
}
