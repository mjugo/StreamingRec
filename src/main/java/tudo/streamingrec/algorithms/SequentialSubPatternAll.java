package tudo.streamingrec.algorithms;

import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import tudo.streamingrec.data.ClickData;
import tudo.streamingrec.data.Transaction;
import tudo.streamingrec.util.Util;

/**
 * An algorithm that extracts and stores sequential patterns to make
 * recommendations. This variety is the least strict. In the learning phase, we
 * extract all sub patterns of each session. And, in the recommendation phase,
 * we match all sub patterns that end with the most current click.
 * 
 * @author Mozhgan
 *
 */
public class SequentialSubPatternAll extends SequentialSubPattern {
	//should we weight pattern scores lower the further their overlap is away 
	//in the session from the current click?
	protected boolean weight = false;
	@Override
	public LongArrayList recommend(ClickData clickData) {
		// create a score map
		Map<Long, Double> score = new Long2DoubleOpenHashMap();
		// iterate over the subpatterns of the session that end with the most
		// current click
		for (int j = 0; j < clickData.session.size(); j++) {
			// step down the pattern tree
			SequenceTreeNode currentNode = patternTree;
			for (int i = j; i < clickData.session.size(); i++) {
				Transaction click = clickData.session.get(i);
				if (!currentNode.children.containsKey(getTreeNodeKey(click))) {
					continue;
				}
				currentNode = currentNode.children.get(getTreeNodeKey(click));
			}
			// if we reached the right node, look at the children and add up the
			// support values
			for (Entry<String, SequenceTreeNode> child : currentNode.children.entrySet()) {
				double childscore = child.getValue().support;
				if (weight){
					childscore = 1d/(j+1)*childscore;
				}
				Long key= Long.parseLong(child.getKey());
				if (score.containsKey(key)) {
					score.put(key, (score.get(key) + childscore));
				}
				score.put(key, childscore);
			}
		}

		// sort the accumulated support values and create a recommendation list
		return (LongArrayList) Util.sortByValueAndGetKeys(score, false, new LongArrayList());
	}

	/**
	 * should we weight pattern scores lower the further their overlap is away 
	 * in the session from the current click?
	 * @param weight -
	 */
	public void setWeight(boolean weight) {
		this.weight = weight;
	}
}
