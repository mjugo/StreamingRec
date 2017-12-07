package tudo.streamingrec.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Merges the temp results of multiple runs into a csv file for manual analysis
 * @author MJ
 *
 */
@Command(name = "CsvResultMerger", 
footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
description = "Merges the (temporary) result files into a csv file for analysis, e.g., in MS Excel. Usage:", 
showDefaultValues = true,
sortOptions = false)
public class CsvResultMerger {
	@Parameters(index = "0..*", paramLabel="<FILES>", description = "The paths to the tmp result files.")
	private static String[] resultFiles = {};
	@Option(names = {"-p", "--publisher-extractor"}, paramLabel="<REGEX>", description = 
			"An optional regex pattern to extract the publisher id from the first few comment lines of the result files,"
			+ "if result files for multiple publishers are used."
			+ "For example, if the input file names follow the pattern Items_<publisher_id>.csv, a pattern \\QItems_\\E(\\d+) can be used. "
			+ "The first capturing group of this pattern is used and parsed to an integer." )
	private static String publisherExtractor = null;
	@Option(names = {"-o", "--out-file"}, paramLabel="<FILE>", description = "Path to the output csv file." )
	private static String outFile = "merged_results.csv";
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		if(args.length==0){
			System.out.println("Help for commandline arguments with -h");
		}
		//command line parsing
		CommandLine.populateCommand(new CsvResultMerger(), args);
		if (helpRequested) {
		   CommandLine.usage(new CsvResultMerger(), System.out);
		   return;
		}
		if(publisherExtractor!=null){
			System.out.println("[DEBUG] Using publisher pattern: " + publisherExtractor);
		}		
		//read the lines of all results files
		List<String> lines = new ArrayList<>();		
		for (String string : resultFiles) {
			lines.addAll(Files.readAllLines(Paths.get(string), Charset.defaultCharset()));
		}

		Map<Integer, Map<String, Map<String, String>>> output = new LinkedHashMap<>();
		int publisher = -1;
		for(String line : lines){
			if(line.startsWith("#")){
				//ignore comments except for optional publisher parsing
				if(line.startsWith("#Input files: ")){
					if(publisherExtractor!=null){
						//extract the publisher, in case in case the file names have a specific pattern
						Matcher matcher = Pattern.compile(publisherExtractor).matcher(line);
						matcher.find();					
						publisher = Integer.parseInt(matcher.group(1));
					}					
				}
				continue;
			}
			//split the result lines
			String[] split = line.split(";");
			//iterate over the metric results
			for(int i = 1; i<split.length; i+=2){
				String metric = split[i];
				String result = split[i+1];
				//add the results to a map per publisher
				Map<String, Map<String, String>> map = output.get(publisher);
				if(map==null){
					map = new LinkedHashMap<>();
					output.put(publisher, map);
				}
				//add the results to a result map
				Map<String, String> list = map.get(metric);
				if(list==null){
					list = new HashMap<String, String>();
					map.put(metric, list);
				}
				list.put(split[0], StringUtils.rightPad(split[0], 70, ' ') + "\t" + result);
			}
		}
		//print some debug output
		System.out.println("[DEBUG] " + output);
		
		//map everything based on publisher, algorithm, and metric name
		List<String> metrics = output.values().stream().flatMap(e -> e.keySet().stream()).distinct().collect(Collectors.toList());
		List<String> algos = output.values().stream().flatMap(e -> e.values().stream()).flatMap(e -> e.values().stream()).map(CsvResultMerger::getAlgo).distinct().collect(Collectors.toList());
		List<Integer> publishers = new ArrayList<>(output.keySet());
		//print some debug output
		System.out.println("[DEBUG] Metrics: " + metrics);
		System.out.println("[DEBUG] Metrics: " + algos);
		//print the actual sorted output
		try(PrintWriter pw = new PrintWriter(outFile)){
			StringBuilder lineBuilder = new StringBuilder();
			lineBuilder.append(";");//leave one column blank for the algorithm name
			for (int i = 0; i < metrics.size(); i++) {
				//output all publishers
				for(int thePublisher : publishers){
					lineBuilder.append(thePublisher);
					lineBuilder.append(";");
				}
			}
			pw.println(lineBuilder.toString());
			
			lineBuilder = new StringBuilder();
			lineBuilder.append(";");//leave one column blank for the algorithm name			
			for(String metric : metrics){
				for (int i = 0; i < publishers.size(); i++) {
					//print all metric names
					lineBuilder.append(metric);
					lineBuilder.append(";");
				}
			}
			pw.println(lineBuilder.toString());
			
			//print the actual results of each algorithm
			for(String algo : algos){
				lineBuilder = new StringBuilder();
				lineBuilder.append(algo);
				lineBuilder.append(";");
				//step through the maps and find the right value
				for(String metric : metrics){
					for(int thePublisher : publishers){
						String out = null;
						Map<String, Map<String, String>> map = output.get(thePublisher);						
						//check if the value is there
						if(map!=null){
							Map<String, String> map2 = map.get(metric);
							if(map2!=null){
								String value = map2.get(algo);
								if(value!=null){
									//we found it 
									out = value.substring(value.indexOf("\t")).trim();									
								}
							}
						}
						//fallback
						if(out==null){
							out = "n/a";
						}
						//print
						lineBuilder.append(out);
						lineBuilder.append(";");
					}
				}
				pw.println(lineBuilder.toString());
			}			
		}
		System.out.println("Output written to \""+outFile+"\"");
	}
	
	/**
	 * extract the algorithm name
	 * @param resultString -
	 * @return the algorithm name
	 */
	private static String getAlgo(String resultString){
		return resultString.substring(0, resultString.indexOf("\t")).trim();
	}
	
}
