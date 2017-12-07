package tudo.streamingrec;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import tudo.streamingrec.algorithms.Algorithm;
import tudo.streamingrec.evaluation.metrics.Metric;

/**
 * Instantiates Algorithm and Metric objects from a json configuration via jackson 
 * @author Mozhgan
 *
 */
public class Config {
	
	/**
	 * Automatically create a list of {@link Algorithm} objects based on a JSON configuration file
	 * @param filename the name of the JSON config file
	 * @return a list of Algorithm objects
	 * @throws JsonParseException -
	 * @throws JsonMappingException -
	 * @throws IOException -
	 */
	public static List<Algorithm> loadAlgorithms(String filename) throws JsonParseException, JsonMappingException, IOException{
		//let jackson create and configure the Algorithm objects
		JsonFactory factory = new JsonFactory();
		factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
		return new ObjectMapper(factory).readValue(new File(filename), new TypeReference<List<Algorithm>>(){});
	}

	/**
	 * Automatically create a list of {@link Metric} objects based on a JSON configuration file
	 * @param filename the name of the JSON config file
	 * @return a list of Metric objects
	 * @throws JsonParseException -
	 * @throws JsonMappingException -
	 * @throws IOException -
	 */
	public static List<Metric> loadMetrics(String filename) throws JsonParseException, JsonMappingException, IOException{
		//let jackson create and configure the Metric objects
		JsonFactory factory = new JsonFactory();
		factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
		return new ObjectMapper(factory).readValue(new File(filename), new TypeReference<List<Metric>>(){});
	}
}
