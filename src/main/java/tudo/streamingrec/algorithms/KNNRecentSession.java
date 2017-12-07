package tudo.streamingrec.algorithms;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.googlecode.javaewah.EWAHCompressedBitmap;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import tudo.streamingrec.data.Transaction;

/**
 * A modification of the standard kNN algorithm that only considers the most
 * recent sessions for comparison. "Old" sessions are removed from consideration
 * either based on the timestamp of their last click (e.g. older than 1 hour) OR
 * they are removed when more than n recent sessions are accumulated.
 * 
 * @author Mozhgan
 *
 */
public class KNNRecentSession extends KNN {
	//a map of recent session
	private Map<Transaction, Date> recentSession = new Object2ObjectLinkedOpenHashMap<Transaction, Date>();
	
	//should the recency of a session be determined based on its (simulation time) age 
	//or based on how many other sessions happen since then
	private boolean timeBased = true;
	//if the (simulation time) age of the session is relevant, how much time should pass until the session is considered "old"?
	private int filterTime = 24 * 60 * 60 * 1000;// default = 1 day
	//if the number of the sessions that happened since each session is relevant, how many sessions should happen until the session is considered "old"?
	private int filterNumber = 10000;

	/**
	 * Changed in comparison to the original to only calculate similarity scores for
	 * the most recent sessions instead of all sessions.
	 */
	@Override
	protected void similarities(EWAHCompressedBitmap uniqueItemscurrentSession,
			Map<EWAHCompressedBitmap, Double> similarityMap) {
		//we only iterate over the recent sessions
		for (Transaction recentSessionID : recentSession.keySet()) {
			EWAHCompressedBitmap recentSessionBitMap = sessionItemID.get(recentSessionID);
			double jaccardS = similarity(uniqueItemscurrentSession, recentSessionBitMap);
			if (jaccardS != 0 && jaccardS != 1) {
				similarityMap.put(recentSessionBitMap, jaccardS);
			}
		}
	}

	/**
	 * Changed compared to the original implementation to create a map of most
	 * recent sessions (based on their ids and timestamps)
	 */
	@Override
	protected EWAHCompressedBitmap convert2BitMap(List<Transaction> session) {
		Transaction key = session.get(0);

		// store the ids and timestamps of the sessions with the most recent
		// clicks in a map
		if (recentSession.containsKey(key)) {
			// dont store ids twice
			recentSession.remove(key);
		}
		recentSession.put(key, key.timestamp);

		// either remove sessions from the cache of most recent sessions
		// based on the timestamp (most recent click is too old)
		// or based on a threshold of a number of most recent sessions.
		if (timeBased) {
			// In case we are working time based, remove sessions from the end of the 
			//linked hash map if they are too old based on their timestamps.
			for (Iterator<Entry<Transaction, Date>> iterator = recentSession.entrySet().iterator(); iterator.hasNext();) {
				Entry<Transaction, Date> entry = iterator.next();
				if (session.get(session.size()-1).timestamp.getTime() - entry.getValue().getTime() > filterTime) {
					iterator.remove();
				} else {
					break;
				}
			}
		} else {
			//In case we limit the number of most recent session based on a fixed number,
			//remove one session from the map, or maybe none if the current session was already in it.
			if (recentSession.size() > filterNumber) {
				Iterator<Entry<Transaction, Date>> iterator = recentSession.entrySet().iterator();
				iterator.next();
				iterator.remove();
			}
		}
		
		if (session.size() == 1) {
			// first click of the current session
			// -> add a new bitset to the map
			sessionItemID.put(key, EWAHCompressedBitmap.bitmapOf(mapItemID(session.get(session.size()-1).item.id)));
		} else {
			// second or later click of the session
			// -> add the current click to the already existing bitset in the
			// map
			sessionItemID.get(key).set(mapItemID(session.get(session.size()-1).item.id));
		}
		// return the bitset
		return sessionItemID.get(key);
	}

	/**
	 * Should we remove sessions from consideration for the nearest neighborhood based on the
	 * timestamps of the clicks in the sessions or based on a fixed number.
	 * E.g. keep exactly 1000 most recent sessions or remove session from the cache 
	 * after 1h indepedent of the current number of most recent sessions
	 * @param timeBased -
	 */
	public void setTimeBased(boolean timeBased) {
		this.timeBased = timeBased;
	}

	/**
	 * Set the time in milliseconds after which sessions are considered old
	 * and removed from the list of recent sessions.
	 * @param filterTime -
	 */
	public void setFilterTime(int filterTime) {
		this.filterTime = filterTime;
	}

	/**
	 * Set the number of sessions that should be considered as most recent sessions.
	 * @param filterNumber -
	 */
	public void setFilterNumber(int filterNumber) {
		this.filterNumber = filterNumber;
	}

}
