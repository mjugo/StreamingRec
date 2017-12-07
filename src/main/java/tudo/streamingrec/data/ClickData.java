package tudo.streamingrec.data;

import java.util.List;

/**
 * Represents useful data associated with one specific click in the dataset
 * @author MJ
 *
 */
public class ClickData {
	//the actual click
	public Transaction click;
	//the session in which this click occurred
	public List<Transaction> session;
	//all of the users previous clicks
	public List<Transaction> wholeUserHistory;
}
