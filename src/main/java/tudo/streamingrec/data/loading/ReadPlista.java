package tudo.streamingrec.data.loading;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tudo.streamingrec.data.Constants;
import tudo.streamingrec.data.Item;

/**
 * Read from Plista data set, February 2016 data log, CLEF2017.
 * Creates two temporary files, that need to be run through {@link JoinPlistaTransactionsWithMetaInfo}.
 * 
 * @author Mozhgan
 */
@Command(name = "ReadPlista", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "Reads the plista data from a set of targz files and converts them to a temporary format. "
			+ "These files then need to be processed with JoinPlistaTransactionsWithMetaInfo. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class ReadPlista {

	//the folder with the plista dataset
	@Option(names = {"-f", "--input-folder"}, paramLabel="<FOLDER>", description = "Path to the input folder with all input files") 
	private static String inputFolder = "plista/";
	//the name of the output file containing the article metadata
	@Option(names = {"-i", "--items"}, paramLabel="<FILE>", description = "Path to the item output file") 
	private static String outputFileNameItems = "data/Items_plista.csv";
	//the name of the output file containing the transactions (clicks)
	@Option(names = {"-c", "--clicks"}, paramLabel="<FILE>", description = "Path to the item output file") 
	private static String outputFileNameClicks = "data/Clicks_plista.csv";
	//should the category be extracted from the article URL? (probably more precise)
	@Option(names = {"-e", "--extract-category"}, description = "Should the category be extracted from the article URL? (probably more precise)") 
	private static boolean extractCATfromURL = false;
	//should the article texts be extracted?
	@Option(names = {"-t", "--extract-text"}, description = "Should the article texts be extracted?") 
	private static boolean extractText = false;
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;
	
	//internal map for translating plain-text categories to integer ids
	private static Map<String, Integer> cat2ID = new HashMap<String, Integer>();
	//a counter for the category id
	private static int lastCATID = 1;

	public static void main(String[] args) throws IOException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new ReadPlista(), args);
		if (helpRequested) {
		   CommandLine.usage(new ReadPlista(), System.out);
		   return;
		}
		// create a file to write Recommendation Request and also event
		// notification
		File foutRR = new File(outputFileNameClicks);
		FileOutputStream fosRR = new FileOutputStream(foutRR);
		BufferedWriter bwRR = new BufferedWriter(new OutputStreamWriter(fosRR));
		bwRR.write("Publisher,Category,ItemID,Cookie,Timestamp,keywords");
		bwRR.newLine();
		// create a file to write Item Update
		File foutIU = new File(outputFileNameItems);
		FileOutputStream fosIU = new FileOutputStream(foutIU);
		BufferedWriter bwIU = new BufferedWriter(new OutputStreamWriter(fosIU));
		bwIU.write("Domain,CreatedAt,ItemID,URL,Title,category,text,keywords");
		bwIU.newLine();
		bwIU.flush();

		// count the processed line
		int lineCount = 0;

		// list the files in the directory
		File file = new File(inputFolder);
		if(!file.exists()){
			System.err.println("Input folder \""+inputFolder +"\" does not exist");
		}
		File[] listfiles = file.listFiles();
		//iterate over the JSON log files line by lines
		for (File plistafile : listfiles) {
			if (plistafile.getName().endsWith(".gz")) {
				System.out.println(plistafile.getName());
				try {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(new GZIPInputStream(new FileInputStream(plistafile))));
					String str = "";
					while ((str = br.readLine()) != null) {
						//initialize an output line which is written to either the item or click file
						String resultLine = "";
						//recommendation requests and event notifications are clicks
						//item updates contain item metadata
						if (str.startsWith("recommendation_request")) {
							//extract the JSON string from the line and parse the click info
							String jsonstr = str.substring(23, str.length() - 24);
							resultLine = readRecommendationRequestOrEventNotification(jsonstr, false);
							//write the extracted info to the click file
							bwRR.write(resultLine);
							bwRR.newLine();
						} else if (str.startsWith("event_notification")) {
							//extract the JSON string from the line and parse the click info
							String jsonstr = str.substring(19, str.length() - 24);
							resultLine = readRecommendationRequestOrEventNotification(jsonstr, true);
							//write the extracted info to the click file
							bwRR.write(resultLine);
							bwRR.newLine();
						} else if (str.startsWith("item_update")) {
							//extract the JSON string from the line and parse the item meta data
							String jsonstr = str.substring(12, str.length() - 24);
							resultLine = readUpdateItem(jsonstr);
							//write the extracted info to the item file
							bwIU.write(resultLine);
							bwIU.newLine();
						}

						// count the lines
						lineCount++;
						if (lineCount % 1000000 == 0) {
							//print progress regularly
							System.out.println("line = " + lineCount / 1000000 + "m");
							bwRR.flush();
							bwIU.flush();
						}
					}
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		bwIU.close();
		bwRR.close();
		System.out.println("End of program!");
	}

	/**
	 * This method extracts the information about a user click from a json
	 * string
	 * 
	 * @param jsonstr -
	 * @param eventNotification -
	 * @return a csv line
	 */
	private static String readRecommendationRequestOrEventNotification(String jsonstr, boolean eventNotification) {
		//parse the JSON sting to a json object
		JSONObject linejson = new JSONObject(jsonstr);
		// context
		JSONObject contextRR = linejson.getJSONObject("context");
		// each context object contains object of type: simple, lists, cluster
		JSONObject simpleRR = contextRR.getJSONObject("simple");
		JSONObject listRR = contextRR.getJSONObject("lists");

		//create a tmp transaction project (click info + some item metadata 
		//that needs to be merged to item objects later)
		TmpPlistaTransaction transaction = new TmpPlistaTransaction();
		// each list object contains a list of tuples
		// code_11 is Category of the news article
		if (listRR.has("11")) {
			JSONArray categoryArray = listRR.getJSONArray("11");
			if (categoryArray.length() > 1) {
				System.err.println("More than one category.");
			} else if (categoryArray.length() == 1) {
				transaction.category = categoryArray.getInt(0);
			}
		}

		// each simple objects contains a list of tuples
		// code_27 is publisher ID
		if (simpleRR.has("27")) {
			transaction.publisher = simpleRR.getInt("27");
		}

		// code_57 is user cookie
		if (simpleRR.has("57")) {
			transaction.cookie = simpleRR.getLong("57");
		}
		
		// code_25 is item ID
		if (simpleRR.has("25")) {
			transaction.item = new Item();
			transaction.item.id = simpleRR.getLong("25");
		}

		// timestamp
		if (linejson.has("timestamp")) {
			transaction.timestamp = new Date(linejson.getLong("timestamp"));
		}

		// Different branch for event notification
		if (eventNotification) {
			// We dont need to differentiate between event notification and
			// recommendation request right now.
			// But maybe in the future. We can do it here.
		}
		
		JSONObject clustersRR = contextRR.getJSONObject("clusters");
		//keywords
		if (extractText && clustersRR.has("33")) {
			Object object = clustersRR.get("33");
			if(!(object instanceof JSONArray)){
				//replace special line-breaking character 0xAD
				JSONObject keywordObj = (JSONObject) object;
				transaction.keywords = new Object2IntOpenHashMap<>();
				for (String key : keywordObj.keySet()) {
					transaction.keywords.addTo(key.replaceAll(",", " ").replaceAll(Character.toString((char) 0xAD), ""), keywordObj.getInt(key));
				}
			}			
		}

		// create CSV line
		return transaction.toString();
	}

	//these pattern are used to extract categories from URLs
	private static Pattern pattern1677 = Pattern.compile("^\\Qhttp://www.tagesspiegel.de/\\E(.+?)/.+/\\d+\\Q.html\\E$");
	private static Pattern pattern418 = Pattern.compile("^\\Qhttp://www.ksta.de/\\E(.+?)/.+\\Q.html\\E$");
	private static Pattern pattern35774 = Pattern.compile("^\\Qhttp://m.sport1.de/\\E(.+?)/20\\d\\d/\\d\\d/");	

	/**
	 * This method extracts an item's meta information (called 'item update' in
	 * Plista dataset) from a json string
	 * 
	 * @param jsonstr -
	 * @return a csv line
	 * @throws ParseException -
	 * @throws JSONException -
	 */
	private static String readUpdateItem(String jsonstr) throws JSONException, ParseException {
		//parse the json string to a json object
		JSONObject linejson = new JSONObject(jsonstr);

		Item item = new Item();
		// domain ID is publisher ID in the recommendation_request and
		// event_notification
		if (linejson.has("domainid")) {
			item.publisher = linejson.getInt("domainid");
		}

		// created_at is the creation time of news article
		if (linejson.has("created_at")) {
			item.createdAt = Constants.DATE_FORMAT.parse(linejson.getString("created_at"));
		}

		// item ID
		if (linejson.has("id")) {
			item.id = linejson.getLong("id");
		}

		// use the URL of the article to extract a category
		if (linejson.has("url")) {
			item.url = linejson.getString("url").replaceAll(",", " ");
			if (extractCATfromURL) {
				//use a different regex pattern for depending on publisher
				Pattern pattern = null;
				if (item.publisher == 1677) {
					pattern = pattern1677;
				} else if (item.publisher == 418) {
					pattern = pattern418;
				}else if (item.publisher == 35774) {
					pattern = pattern35774;
				}
				//if the pattern matches assign an integer ID to the plain-text category string
				if (pattern != null) {
					Matcher matcher = pattern.matcher(item.url);
					if (matcher.find()) {
						String extractedCAT = matcher.group(1);
						if (!cat2ID.containsKey(extractedCAT)) {
							cat2ID.put(extractedCAT, lastCATID);
							lastCATID++;
							System.out.println("Publisher: "+ item.publisher + ". Category id: " 
									+ (lastCATID-1) + " -> " + extractedCAT);
						}
						//assign the extracted category id to the item
						item.category = cat2ID.get(extractedCAT);
					}
				}
			}

		}

		// Title of news article
		if (linejson.has("title")) {
			//replace special line-breaking character 0xAD
			item.title = linejson.getString("title").replaceAll(",", " ").replaceAll(Character.toString((char) 0xAD), "");
		}
		
		// Title of news article
		if (extractText && linejson.has("text")) {
			//replace special line-breaking character 0xAD
			item.text = linejson.getString("text").replaceAll(",", " ").replaceAll(Character.toString((char) 0xAD), "");
		}
		
		// create CSV line
		return item.toString();
	}
}