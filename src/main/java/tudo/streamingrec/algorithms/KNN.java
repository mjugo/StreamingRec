package tudo.streamingrec.algorithms;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.googlecode.javaewah.EWAHCompressedBitmap;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * Session kNN algorithm. Calculates the similarity between two maps in
 * terms of the Jaccard similarity of the set of items that were clicked in each session.
 * If a session is very similar to the current session and is thus in the top-N neighbors list,
 * the item that were clicked within this session are used as recommendation candidates.
 * @author Mozhgan
 *
 */
public class KNN extends Algorithm {
	//the number of neighbors
	protected int k = 5;

	//maps each item's LONG id to an INT, since EWAHCompressedBitmap can only store INT
	private Map<Long, Integer> itemID = new Long2IntOpenHashMap();
	//a counter that points to the nex int item id to be used
	private int itemIDcounter = 0;
	//a reverse lookup of the "itemID" map, so that the actual LONG item IDs can be recreated
	protected Int2LongOpenHashMap itemMap = new Int2LongOpenHashMap();	
	//the actual list of last sessions stored in bitmap format.
	//the first click of each session is used as a key so that the following clicks can be attributed to the right session.
	protected Map<Transaction, EWAHCompressedBitmap> sessionItemID = new Object2ObjectOpenHashMap<Transaction, EWAHCompressedBitmap>();

	@Override
	public void trainInternal(List<Item> items, List<ClickData> clickData) {
		//for each click in the training data
		for (ClickData c : clickData) {
			// store all items of the current session in a bit set
			convert2BitMap(c.session);
			// map items to their temporary (0-indexed) INT ids
			itemMap.put(mapItemID(c.click.item.id), c.click.item.id);
		}
	}

	@Override
	public LongArrayList recommend(ClickData clickData) {
		// extract the current user session as a bit set from our storage
		EWAHCompressedBitmap uniqueItemscurrentSession = convert2BitMap(clickData.session);

		// create a map of similarity scores
		Map<EWAHCompressedBitmap, Double> similarityMap = new Object2DoubleOpenHashMap<EWAHCompressedBitmap>();

		// iterate over all other session, calculate the jaccard index, and save
		// it in the
		// similarity score map
		similarities(uniqueItemscurrentSession, similarityMap);

		// sort the sessions by their similarity score
		similarityMap = Util.sortByValue(similarityMap, false);

		// create a map of item scores
		Map<Integer, Double> itemScore = new Int2DoubleOpenHashMap();

		// iterate over the k nearest neighbor sessions
		int neighborCounter = 0;
		for (Entry<EWAHCompressedBitmap, Double> session : similarityMap.entrySet()) {
			neighborCounter++;
			// find the items from the neighbor session that are not contained
			// in the current session
			EWAHCompressedBitmap recommendableItems = session.getKey().xor(uniqueItemscurrentSession)
					.and(session.getKey());
			for (Integer item : recommendableItems) {
				// sum up the item scores
				Double sum = itemScore.get(item);
				if (sum == null) {
					sum = 0d;
				}
				itemScore.put(item, session.getValue() + sum);
			}
			// stop at k neighbors
			if (neighborCounter > k) {
				break;
			}
		}

		// sort the item scores
		itemScore = Util.sortByValue(itemScore, false);

		// remap all item ids back to the actual Item objects
		LongArrayList sortedItems = new LongArrayList();
		for (Integer itemId : itemScore.keySet()) {
			sortedItems.add(itemMap.get((int)itemId));
		}
		return sortedItems;
	}

	/**
	 * Calculates the similarities between the current session and the
	 * other sessions and stores the similarity scores in a map
	 * @param uniqueItemscurrentSession -
	 * @param similarityMap -
	 */
	protected void similarities(EWAHCompressedBitmap uniqueItemscurrentSession,
			Map<EWAHCompressedBitmap, Double> similarityMap) {
		for (EWAHCompressedBitmap anotherSession : sessionItemID.values()) {
			double jaccardS = similarity(uniqueItemscurrentSession, anotherSession);
			if (jaccardS != 0 && jaccardS != 1) {
				similarityMap.put(anotherSession, jaccardS);
			}
		}

	}

	/**
	 * Calculates the jaccard similarity between two bit sets that represent the
	 * current and another user session's unique item ids
	 * 
	 * @param uniqueItemscurrentSession -
	 * @param anotherSession -
	 * @return the sim
	 */
	protected double similarity(EWAHCompressedBitmap uniqueItemscurrentSession, EWAHCompressedBitmap anotherSession) {
		int intersection = uniqueItemscurrentSession.andCardinality(anotherSession);
		if (intersection == 0) {
			// if the intersection is 0 -> return 0
			return 0;
		}
		// otherwise calculate the jaccard
		int union = uniqueItemscurrentSession.orCardinality(anotherSession);
		return intersection * 1d / union * 1d;
	}

	/**
	 * Converts a session (based on the current click of the session) into a bit
	 * set to make the similarity calculation easier.
	 * @param session  -
	 * @return the bitmap
	 */
	protected EWAHCompressedBitmap convert2BitMap(List<Transaction> session) {
		if (session.size() == 1) {
			// first click of the current session
			// -> add a new bitset to the map
			//(hash based on first transaction in session)
			sessionItemID.put(session.get(0),
					EWAHCompressedBitmap.bitmapOf(mapItemID(session.get(0).item.id)));
		} else {
			// second or later click of the session
			// -> add the current click to the already existing bitset in the
			// map
			sessionItemID.get(session.get(0)).set(mapItemID(session.get(session.size()-1).item.id));
		}
		// return the bitset
		return sessionItemID.get(session.get(0));
	}

	/**
	 * Maps an item id from the original data set (long) to a zero-indexed
	 * integer
	 * 
	 * @param item -
	 * @return the assigned item id as an integer
	 */
	protected int mapItemID(long item) {
		if (itemID.containsKey(item)) {
			return itemID.get(item);
		}
		itemID.put(item, itemIDcounter);
		return itemIDcounter++;
	}

	/**
	 * Sets the number of nearest neighbors to consider
	 * @param k -
	 */
	public void setK(int k) {
		this.k = k;
	}
}
