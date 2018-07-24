package tudo.streamingrec.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import tudo.streamingrec.AlgorithmWrapper;
import tudo.streamingrec.evaluation.metrics.HypothesisTestableMetric;

/**
 * Retroactively, i.e., after the actual execution of the program, run statistical test on the results
 * @author MJ
 *
 */
@Command(name = "RetroactiveStatisticalTests", 
	footer = "Copyright(c) 2017 Mozhgan Karimi, Michael Jugovac, Dietmar Jannach",
	description = "Executes statistical (t-)tests after the main evaluation has already run. "
			+ "Can also be used to combine statistical test of multiple identically configured evaluation runs or"
			+ "to run statistical test on for evaluations that did not finish for all algorithms. Usage:", 
	showDefaultValues = true,
	sortOptions = false)
public class RetroactiveStatisticalTests {
	//the input folder with all stat result files that are to be tested.
	@Parameters(index = "0", paramLabel="<FOLDER>", description = "The folder where the statistical evaluation output file were saved to.")
	private static String folder = "stats/";
	
	//wilcoxon test instead of t-test?
	@Option(names = {"-s", "--smirnov"}, description = "Uses a Kolmogorov Smirnov test instead of a paired t-test.")
	private static boolean smirnov = false;
	
	//for command line help
	@Option(names = {"-h", "--help"}, hidden=true, usageHelp = true)
	private static boolean helpRequested;

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		if(args.length>0){
			//command line parsing
			CommandLine.populateCommand(new RetroactiveStatisticalTests(), args);
			if (helpRequested) {
			   CommandLine.usage(new RetroactiveStatisticalTests(), System.out);
			   return;
			}
		}else{
			System.out.println("Help for commandline arguments with -h");
		}
		//create a list of metrics
		List<HypothesisTestableMetric> metrics = new ArrayList<>();
		//list all stat result files in the folder
		if(!new File(folder).exists()){
			System.err.println("Folder \""+ folder +"\" does not exist.");
			return;
		}
		File[] listFiles = new File(folder).listFiles(new FilenameFilter() {			
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(AlgorithmWrapper.statPrefix);
			}
		});
			
		//iterate over each file and deserialize the detailed metric results
		for(File file : listFiles){
			try(FileInputStream fi = new FileInputStream(file);
					ObjectInputStream oi = new ObjectInputStream(fi)){
				// Read objects
				while( fi.available() > 0) // check if the file stream is at the end
				{
					metrics.add((HypothesisTestableMetric) oi.readObject());
				}
			}
		}
		System.out.println("Starting tests");
		//execute the statistical t-test and print to console
		System.out.println(Util.executeStatisticalTests(metrics, smirnov));
	}
}
