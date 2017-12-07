package tudo.streamingrec.data.loading;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * Reads the item meta information from the (intermediate) CSV file and joins the transactions with it.
 * This step is necessary for the Plista dataset because, unnecessarily, categories, publisher, and keywords 
 * are stored in every click object instead of the item event metadata. So, in this step we join this item meta information
 * to the item objects and create simple transaction objects without meta information about items.
 * 
 * @author Mozhgan
 *
 */
@Command(name = "JoinPlistaTransactionsWithMetaInfo", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "Joins the item info for plista datasets to create the final input files. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class JoinPlistaTransactionsWithMetaInfo {
	//the item input file
	@Option(names = {"-i", "--items"}, paramLabel="<FILE>", description = "Path to the item input file in CSV format") 
	private static String inputFileNameItems = "data/Items_plista.csv";
	//the item click event file
	@Option(names = {"-c", "--clicks"}, paramLabel="<FILE>", description = "Path to the clicks input file in CSV format") 
	private static String inputFileNameClicks = "data/Clicks_plista.csv";
	//optional: a subcategory input file (can be extracted from the url or crawled article content of the publisher; format = "itemid;categoryid;category_level1;category_level2;...")
	@Option(names = {"-s", "--sub-categories"}, paramLabel="<FILE>", description = "An optional subcategory input file (can be extracted from the url or crawled article content of the publisher; format = \"itemid;categoryid;category_level1;category_level2;...\"") 
	private static String inputFileNameSubCategories = null;
	//which publisher should we select from the data?
	@Option(names = {"-p", "--publisher"}, paramLabel="<VALUE>", description = "which publisher should we select from the data?") 
	private static int publisherFilter = 418;
	//the item output file
	@Option(names = {"-I", "--out-items"}, paramLabel="<FILE>", description = "Path to the item output file") 
	private static String outputFileNameItemUpdate = "data/Items.csv";
	//the click event output file
	@Option(names = {"-C", "--out-clicks"}, paramLabel="<FILE>", description = "Path to the clicks output file") 
	private static String outputFileNameRecommendationRequest = "data/Clicks.csv";
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;

	public static void main(String[] args) throws IOException, ParseException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new JoinPlistaTransactionsWithMetaInfo(), args);
		if (helpRequested) {
		   CommandLine.usage(new JoinPlistaTransactionsWithMetaInfo(), System.out);
		   return;
		}
		//,first read the items file
		BufferedReader br = new BufferedReader(new FileReader(inputFileNameItems));
		String str = "";
		br.readLine();//discard header
		
		Map<Long, Item> items = new HashMap<Long, Item>();		
		while ((str = br.readLine()) != null) {
			Item i = new Item(str);//read the items
			if(i.publisher == publisherFilter){//filter by publisher 418
				items.put(i.id, i);//add the items to a map based on the item id as a key
			}			
		}		
		br.close();		
		System.out.println("Number of items: " + items.size());
		
		//second read the transactions file
		List<TmpPlistaTransaction> transactions = new ArrayList<TmpPlistaTransaction>();		
		BufferedReader brT = new BufferedReader(new FileReader(inputFileNameClicks));
		String strT = "";
		//discard header
		brT.readLine();
		//create some missing data statistics
		int overAllcnt = 0;
		int missingCnt = 0;
		int missingCat = 0;
		int missingCatWoMissingItem = 0;
		int missingItem = 0;
		int missingUser = 0;
		int missingItemWithoutMissingCookie = 0;
		//iteratively read the file
		while ((strT = brT.readLine()) != null) {	
			//read tmp plista transaction object
			//we give the item map as an input so that the transactions can be mapped to the right item
			TmpPlistaTransaction transaction = new TmpPlistaTransaction(strT, items);
			if(transaction.publisher == publisherFilter ){//filter by publisher
				overAllcnt++;
				if(transaction.category != 0 &&
						transaction.cookie != 0 &&
						transaction.item != null){ // filter out empty fields (null)
					transactions.add(transaction);//if nothing important is missing, add it to the list
					if(transaction.keywords!=null){
						//if keywords are present, read them from the file
						if(transaction.item.keywords == null){
							transaction.item.keywords = new Object2IntOpenHashMap<>();
						}
						for (String keyword : transaction.keywords.keySet()) {
							transaction.item.keywords.addTo(keyword, transaction.keywords.getInt(keyword));
						}
					}
				}else{
					//if some important field is missing, count it
					missingCnt++;
					if(transaction.category==0){
						missingCat++;
						if(transaction.item!=null){
							missingCatWoMissingItem++;
						}
					}
					if(transaction.cookie == 0){
						missingUser++;
					}
					if(transaction.item == null){
						missingItem++;
						if(transaction.cookie != 0){
							missingItemWithoutMissingCookie++;
						}
					}
					
				}
			}				
		}		
		brT.close();
		//output the overall and missing data statistics
		System.out.println("Number of transaction: " + transactions.size());
		
		System.out.println("Overall lines: "+overAllcnt);
		System.out.println("Filtered because of missing data: " + missingCnt);
		System.out.println("Missing category: " + missingCat);
		System.out.println("Missing category w/o missing item: " + missingCatWoMissingItem);
		System.out.println("Missing item: " + missingItem);
		System.out.println("Missing category w/o missing user: " + missingItemWithoutMissingCookie);
		System.out.println("Missing user: " + missingUser);
		
		//if categories were crawled from the item URLs, map those categories to the items
		//why are we doing this: categories for the publisher 418 from the dataset are far too specific. 
		if(inputFileNameSubCategories!=null){
			BufferedReader brSc = new BufferedReader(new FileReader(inputFileNameSubCategories));
			String strSc = "";
			//discard header
			brSc.readLine();
			//get the plain-text category for each category id from the data set
			Map<Integer, Map<String, Integer>> superCategoryMap = new HashMap<>();		
			while ((strSc = brSc.readLine()) != null) {
				String[] split = strSc.split(";");
				if(split.length<4){
					continue;
				}
				//extact category id (plista)
				int categoryID = Integer.parseInt(split[1]);
								String superCategory = split[3];
				//extract corresponding plain-text category
				if(superCategory!=null&&!superCategory.isEmpty()){
					Map<String, Integer> map = superCategoryMap.get(categoryID);
					if(map == null){
						map = new HashMap<>();
						superCategoryMap.put(categoryID, map);
					}
					Integer count = map.get(superCategory);
					if(count == null){
						count = 0;
					}
					map.put(superCategory, count+1);
				}
			}
			brSc.close();
			int counter = 1;
			//assign category IDs to the plain-text categories from the publisher websites
			Map<String, Integer> categoryIdMap = new HashMap<>();	
			Map<Integer, Integer> finalSuperCategoryMap = new HashMap<>();	
			for (Entry<Integer, Map<String, Integer>> entry : superCategoryMap.entrySet()) {
				Map<String, Integer> sorted = Util.sortByValue(entry.getValue(), false);
				String bestSuperCategory = sorted.keySet().iterator().next();
				if(!categoryIdMap.containsKey(bestSuperCategory)){
					categoryIdMap.put(bestSuperCategory, counter++);
				}
				finalSuperCategoryMap.put(entry.getKey(), categoryIdMap.get(bestSuperCategory));
			}
		
			
			for(Item i : items.values()){
				//map the new category ids to the items
				if(finalSuperCategoryMap.containsKey(i.category)){
					i.category = finalSuperCategoryMap.get(i.category);
				}
				//put the categories in the keyword map (since for 418 not many items have keywords)
				if(i.keywords==null){
					i.keywords = new Object2IntOpenHashMap<>();
				}
				i.keywords.put(i.category+"cat", 1);
			}
		}
		
		//convert to "normal" transaction format
		List<Transaction> mappedTransactions = transactions.stream().map(t -> t.toTransaction()).collect(Collectors.toList());
		
		//write the filtered csv files
		// create a file to write Recommendation Request and also event notification
		Util.writeTransactions(mappedTransactions, outputFileNameRecommendationRequest);
		
		// create a file to write Item Update
		Util.writeItems(items.values(), outputFileNameItemUpdate);
	}
}
