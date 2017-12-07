package tudo.streamingrec.data;

import java.util.Date;

/**
 * An interface that combines the common attribute of items and transactions,
 * the time stamp, to make them sortable in one list.
 * @author Mozhgan
 *
 */
public interface Event {
	/**
	 * Returns the time stamp of a click or the publication time of an item
	 * @return the event time
	 */
	public Date getEventTime();
}
