package tudo.streamingrec.data.session;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import tudo.streamingrec.data.Transaction;

/**
 * Creates sessions from time-ordered transactions based on the user id and the
 * time between transactions.
 * 
 * @author Mozhgan
 *
 */
public class SessionExtractor {
	private static boolean sessionInactivityThreshold;
	private static long thresholdInMS;

	//the storage for the sessions
	private Map<Long, List<List<Transaction>>> sessions = new Long2ObjectOpenHashMap<List<List<Transaction>>>();

	/**
	 * Adds one click to the session storage and assigns it to the corresponding
	 * user session. This method needs to be called in order (time-wise).
	 * 
	 * @param t the current click
	 * @return the session
	 */
	public List<Transaction> addClick(Transaction t) {
		if (!sessions.containsKey(t.userId)) {
			// The user has no previous clicks or sessions
			// -> create a list of sessions with one session in it and add the
			// current click
			List<List<Transaction>> sessionList = new ObjectArrayList<List<Transaction>>();
			sessions.put(t.userId, sessionList);
			List<Transaction> session = new ObjectArrayList<Transaction>();
			session.add(t);
			sessionList.add(session);
			// we are done for this case. Do not continue.
			return session;
		}
		// if the user had previous clicks, get the list of session and retrieve
		// the last session from it.
		List<List<Transaction>> sessionList = sessions.get(t.userId);
		List<Transaction> lastSession = sessionList.get(sessionList.size() - 1);
		if ((isSessionInactivityThreshold() && (t.timestamp.getTime()
				- lastSession.get(lastSession.size() - 1).timestamp.getTime() <= getThresholdInMS()))
				|| (!isSessionInactivityThreshold()
						&& (DateUtils.isSameDay(lastSession.get(lastSession.size() - 1).timestamp, t.timestamp)))) {
			// if the difference between the last click event of the last
			// session
			// and the current click is less than N milliseconds,
			// add it at the end of the current session
			lastSession.add(t);
			return lastSession;
		} else {
			// else, create a new session and add the click
			List<Transaction> session = new ObjectArrayList<Transaction>();
			session.add(t);
			sessionList.add(session);
			return session;
		}
	}

	/**
	 * Finds and returns the specific session in which this click occured
	 * @param t the click
	 * @return the session for the click
	 */
	public List<Transaction> getSession(Transaction t) {
		//first, find the all sessions for this user
		List<List<Transaction>> sessionList = sessions.get(t.userId);
		//iterate over the user sessions and find the right click
		for (List<Transaction> list : sessionList) {
			for (Transaction transactionInList : list) {
				if (transactionInList == t) {
					//reference comparison should be fine, since we do not copy transactions
					return list;
				}
			}
		}
		//we did not find anything -> return null
		return null;
	}

	

	/**
	 * Returns a list of all sessions mapped by user ID
	 * @return all sessions so far mapped by user ID
	 */
	public Map<Long, List<List<Transaction>>> getSessionMap() {
		return sessions;
	}

	/**
	 * true = sessions are created based on the idle time between two events. 
	 * The actual threshold that is used is {{@link #thresholdInMS}.
	 * false = every user click in one day is assigned to the same session. 
	 * @return if a session inactivity thereshold should be used
	 */
	public static boolean isSessionInactivityThreshold() {
		return sessionInactivityThreshold;
	}

	/**
	 * true = sessions are created based on the idle time between two events. 
	 * The actual threshold that is used is {{@link #thresholdInMS}.
	 * false = every user click in one day is assigned to the same session. 
	 * @param sessionInactivityThreshold -
	 */
	public static void setSessionInactivityThreshold(boolean sessionInactivityThreshold) {
		SessionExtractor.sessionInactivityThreshold = sessionInactivityThreshold;
	}

	/**
	 * Determines the threshold in milliseconds for session cohesion, i.e.,
	 * the maximum allowed time between two clicks of one user to be considered
	 * part of one session.
	 * @return the sessions threshold
	 */
	public static long getThresholdInMS() {
		return thresholdInMS;
	}

	/**
	 * Determines the threshold in milliseconds for session cohesion, i.e.,
	 * the maximum allowed time between two clicks of one user to be considered
	 * part of one session.
	 * 
	 * @param thresholdInMS -
	 */
	public static void setThresholdInMS(long thresholdInMS) {
		SessionExtractor.thresholdInMS = thresholdInMS;
	}
}
