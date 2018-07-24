package tudo.streamingrec.algorithms;

import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Item;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * An algorithm that extracts and stores sequential patterns to make recommendations.
 * This variant is very strict. It only learns one pattern per session and only
 * recommends items if the pattern in the database matches the whole session exactly.
 * @author Mozhgan
 *
 */
public class SequentialPattern extends Algorithm {
	//a tree data structure that stores the patterns
	protected SequenceTreeNode patternTree = new SequenceTreeNode();

	@Override
	protected void trainInternal(List<Item> items, List<ClickData> clickData) {
		// for all transactions, update the pattern tree
		for (ClickData c : clickData) {
			updateMap(c.session);
		}
	}

	@Override
	public LongArrayList recommendInternal(ClickData clickData) {
		//step down the pattern tree to match the right pattern
		SequenceTreeNode currentNode = patternTree;
		for (Transaction click : clickData.session) {
			if (!currentNode.children.containsKey(getTreeNodeKey(click))) {
				return new LongArrayList();
			}
			currentNode = currentNode.children.get(getTreeNodeKey(click));
		}
		
		//if we found the right pattern, sort the possible completions of this pattern 
		//by their support values and create a recommendation list
		Map<String, SequenceTreeNode> sortMap = Util.sortByValue(currentNode.children, false);
		LongArrayList returnList = new LongArrayList();
		for (String i : sortMap.keySet()){
			returnList.add(Long.parseLong(i));
		}
		return returnList;
	}

	/**
	 * Updates the support values of the (and potentially adds nodes to the) pattern tree 
	 * based on patterns extracted from the current session
	 * @param session  -
	 */
	protected void updateMap(List<Transaction> session) {
		SequenceTreeNode currentNode = patternTree;
		//step down the pattern tree based on the pattern of the session
		for (int i = 0; i < session.size(); i++) {
			Transaction click = session.get(i);
			if (currentNode.children.containsKey(getTreeNodeKey(click))) {
				currentNode = currentNode.children.get(getTreeNodeKey(click));
			} else {
				SequenceTreeNode node = new SequenceTreeNode();
				currentNode.children.put(getTreeNodeKey(click), node);
				currentNode = node;
			}
			//increase the support value of the last node
			if (i == session.size() - 1) {
				currentNode.support++;
			} else if (i == session.size() - 2) {
				//decrease the support of the second-to-last node
				//(because of incremental learning, we assume each time that the session is over
				//and add the complete pattern. If the pattern was incomplete, we decrease the support
				//and increase the support for the full pattern here)
				currentNode.support--;
			}
		}
	}
	
	/**
	 * A method that extracts the relevant information from the transactions that is 
	 * used to build the pattern. In the default implementation, the item id is used.
	 * This produces a pattern database of consecutive item clicks. Child classes may
	 * override this method and use different information to build patterns, e.g., the category.
	 * @param t -
	 * @return the key
	 */
	protected String getTreeNodeKey(Transaction t){
		return ""+ t.item.id;
	}
	
	/**
	 * Represents one tree node in a pattern tree (i.e. a database used to store patterns)
	 * @author Mozhgan
	 *
	 */
	public class SequenceTreeNode implements Comparable<SequenceTreeNode>{
		Map<String,SequenceTreeNode> children = new Object2ObjectOpenHashMap<String,SequenceTreeNode>();
		int support=0;
		public int compareTo(SequenceTreeNode o) {
			return this.support-o.support;
		}

	}
}

