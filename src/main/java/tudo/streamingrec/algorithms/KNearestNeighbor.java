package tudo.streamingrec.algorithms;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * A kNN implementation with a range of different similarity scoring functions
 * @author MJ
 *
 */
public class KNearestNeighbor extends Algorithm{	
	//maps each item id to a list of sessions, which are further indexed by the timestamp of the first click (to update sessions easier)
	Long2ObjectOpenHashMap<Object2ObjectOpenHashMap<Date, List<List<Transaction>>>> itemToSessionByTimeMap = new Long2ObjectOpenHashMap<>();

	private int filterNumber = 1000;
	private int k = 500;
	private ScoringMethod scoringMethod = ScoringMethod.DecayVector;
	
	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clicks) {
		//iterate over sessions
		for (ClickData clickData : clicks) {
			//iterate over clicks in session
			for(Transaction click : clickData.session){
				//find possible maps where this session needs to be stored
				Object2ObjectOpenHashMap<Date,List<List<Transaction>>> itemMap = itemToSessionByTimeMap.get(click.item.id);
				if(itemMap == null){
					//if the map does not exist, create it
					itemMap = new Object2ObjectOpenHashMap<>();
					itemToSessionByTimeMap.put(click.item.id, itemMap);
				}
				List<List<Transaction>> possiblePreviousSessions 
					= itemMap.get(clickData.session.get(0).timestamp); 	//check for possible previous session by using 
																		//this session's first item's timestamp as an identifier
				if(possiblePreviousSessions==null){
					//if the list does not exist, create it
					possiblePreviousSessions = new ArrayList<>();
					itemMap.put(clickData.session.get(0).timestamp, possiblePreviousSessions);
				}
				
				for (Iterator<List<Transaction>> iterator = possiblePreviousSessions.iterator(); iterator.hasNext();) {
					List<Transaction> possiblePreviousSession = iterator.next();
					if(possiblePreviousSession.get(0).userId==click.userId){//user and time are the same as a previous session?
						iterator.remove(); //(part of) the session is known -> remove/deduplicate
						//this also keeps the session from being added twice because of duplicate items in one session.
					}
				}
				
				//add the current session to the appropriate item's map
				possiblePreviousSessions.add(clickData.session);
			}
		}
	}

	@Override
	protected LongArrayList recommendInternal(ClickData clickData) {
		List<Long> itemsInSession =  clickData.session.stream()
				.sorted((a,b) -> b.timestamp.compareTo(a.timestamp))//reverse order to retain last occurrence of each item
				.map(c -> c.item.id)//get item id of each click
				.distinct()//make distinct
				.collect(Collectors.toList());//collect to make available temporarily
		Map<Long, Double> score = itemsInSession.stream()
			.flatMap(i -> itemToSessionByTimeMap.containsKey(i.longValue())?
					itemToSessionByTimeMap.get(i.longValue()).values().stream()//get session that contain this item
					:Stream.empty())//or otherwise nothing
			.flatMap(v -> v.stream())//same as the step before; this is just in case two sessions have the same timestamp
			.sorted((a,b) -> b.get(0).timestamp.compareTo(a.get(0).timestamp))//sort inversely (newest first)
			.limit(filterNumber)//take the N newest sessions
			.map(n -> n.stream()//timestamps not needed anymore -> convert to set of item ids
					.map(t -> t.item.id)//map to item id
					.distinct()//make distinct
					.collect(Collectors.toSet()))			
			.map(n -> similarity(n, itemsInSession))//calculate similarity of potential neighbors
			.filter(n -> n.getValue() != 0 || n.getValue() != 1) //no exact overlap and no non-overlap
			.sorted((a,b) -> Double.compare(b.getValue(), a.getValue()))//sort by similarity (highest first)
			.limit(k)//take k nearest neighbors
			.flatMap(n -> n.getKey().stream()//flat map each item of the neighbor sessions
					.filter(i -> !itemsInSession.contains(i))//item not already in session
					.map(i -> new AbstractMap.SimpleEntry<Long, Double>(i, n.getValue())))	
			.collect(Collectors.groupingBy(n -> n.getKey(), Collectors.summingDouble(n -> n.getValue())));
			//group by key and sum up
		//sort the results and return
		LongArrayList result = new LongArrayList();
		Util.sortByValueAndGetKeys(score, false, result);
		return result;
	}


	/**
	 * Calculates the similarity score between a neighbor session and the current session.
	 * @param itemsInNeighborSession -
	 * @param itemsInCurrentSession -
	 * @return -
	 */
	private Entry<Set<Long>, Double> similarity(Set<Long> itemsInNeighborSession, List<Long> itemsInCurrentSession) {
		SimpleEntry<Set<Long>, Double> retVal = new SimpleEntry<Set<Long>, Double>(itemsInNeighborSession, 0d);
		if(itemsInNeighborSession.isEmpty() && itemsInCurrentSession.isEmpty()){
			return retVal; //to avoid divisionbyzero exception, return 0 here.
		}
			
		if(scoringMethod==ScoringMethod.DecayVector){
			retVal.setValue(decayVectorScore(itemsInNeighborSession, itemsInCurrentSession));
		}else if(scoringMethod==ScoringMethod.Jaccard){
			double intersection = intersection(itemsInNeighborSession, itemsInCurrentSession);
			if(intersection == 0){
				return retVal;//just to speed things up -> return 0
			}
			// union cannot be 0, because we already checked that in the very beginning
			retVal.setValue(intersection / union(itemsInNeighborSession, itemsInCurrentSession));
 		}else if(scoringMethod == ScoringMethod.Cosine){
 			double intersection = intersection(itemsInNeighborSession, itemsInCurrentSession);
			if(intersection == 0){
				return retVal;//just to speed things up -> return 0
			}
			retVal.setValue(intersection / cosineDenominator(itemsInNeighborSession, itemsInCurrentSession));
 		}else if(scoringMethod==ScoringMethod.DecayVectorJaccard||scoringMethod==ScoringMethod.DecayVectorCosine){
 			double decayVectorValue = decayVectorScore(itemsInNeighborSession, itemsInCurrentSession);
 			if(decayVectorValue == 0){
				return retVal;//just to speed things up -> return 0
			}
 			if(scoringMethod==ScoringMethod.DecayVectorJaccard){
 				retVal.setValue(decayVectorValue / union(itemsInNeighborSession, itemsInCurrentSession));
 			}else if(scoringMethod==ScoringMethod.DecayVectorCosine){
 				retVal.setValue(decayVectorValue / cosineDenominator(itemsInNeighborSession, itemsInCurrentSession));
 			} 			
 		}

		return retVal;
	}
	
	/**
	 * Calculates the denominator of the cosine similarity formula
	 * @param itemsInNeighborSession -
	 * @param itemsInCurrentSession -
	 * @return -
	 */
	private double cosineDenominator(Set<Long> itemsInNeighborSession, List<Long> itemsInCurrentSession) {
		return Math.sqrt(itemsInNeighborSession.size()) * Math.sqrt(itemsInCurrentSession.size());
	}

	/**
	 * Calculates the cardinality of the intersection of two item sets
	 * @param itemsInNeighborSession - 
	 * @param itemsInCurrentSession -
	 * @return - 
	 */
	private double intersection(Set<Long> itemsInNeighborSession, List<Long> itemsInCurrentSession) {
		HashSet<Long> intersection = new HashSet<>(itemsInNeighborSession);
		intersection.retainAll(itemsInCurrentSession);
		return intersection.size() * 1d;
	}
	
	/**
	 *  Calculates the cardinality of the union of two item sets
	 * @param itemsInNeighborSession - 
	 * @param itemsInCurrentSession -
	 * @return -
	 */
	private double union(Set<Long> itemsInNeighborSession, List<Long> itemsInCurrentSession) {
		HashSet<Long> union = new HashSet<>(itemsInNeighborSession);
		union.addAll(itemsInCurrentSession); 
		return union.size() * 1d;
	}

	/**
	 * Calculates the similarity score between two item sets based on weighted decay scheme
	 * @param itemsInNeighborSession - 
	 * @param itemsInCurrentSession -
	 * @return -
	 */
	private double decayVectorScore(Set<Long> itemsInNeighborSession, List<Long> itemsInCurrentSession) {
		double score = 0;
		for (int i = 0; i < itemsInCurrentSession.size(); i++) {
			if(itemsInNeighborSession.contains(itemsInCurrentSession.get(i))){
				score += 1 / (i+1); //scoring method 
				//note! itemsInCurrentSession contains the items in reverse order as they appear in the session.
			}
		}
		return score;
	}

	/**
	 * Set the number of most recent sessions with which to compare
	 * @param filterNumber -
	 */
	public void setFilterNumber(int filterNumber) {
		this.filterNumber = filterNumber;
	}


	/**
	 * Set the number of neighbors
	 * @param k -
	 */
	public void setK(int k) {
		this.k = k;
	}

	/**
	 * Sets the scoring method of the internal similarity calculation between sessions.
	 * @param scoringMethod the scoringMethod to set
	 */
	public void setScoringMethod(ScoringMethod scoringMethod) {
		this.scoringMethod = scoringMethod;
	}

	/**
	 * The available option for the similarity scoring method
	 * @author MJ
	 *
	 */
	public static enum ScoringMethod{
		DecayVector, //weights overlap in sessions higher if the item in questions was clicked more recently
		DecayVectorCosine, //like decay vector, but with a denominator taken from the cosine sim
		DecayVectorJaccard, //like decay vector, but with a denominator taken from the jaccard sim
		Cosine,//traditional cosine similarity
		Jaccard//traditional jaccard similarity
	}
}
