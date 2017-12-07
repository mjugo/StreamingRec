package tudo.streamingrec.algorithms;

import java.util.List;

import tudo.streamingrec.data.Transaction;

/**
 * An algorithm that extracts and stores sequential patterns to make recommendations.
 * Compared to the super class, this variant is not so strict in the learning phase.
 * All sub patterns of each session are learned. But for the recommendation phase,
 * the match still has to be perfect.
 * @author Mozhgan
 *
 */
public class SequentialSubPattern extends SequentialPattern {
	
	@Override
	protected void updateMap(List<Transaction> session) {
		//extract the necessary sub patterns from the session and add them to the pattern tree
		//note: some patterns have already been added because (due to the incremental learning) the
		//session was already processed before, but without the most recent click.
		for (int j = 0; j < session.size() - 1; j++) {
			SequenceTreeNode currentNode = patternTree;
			//step down the pattern tree to add the pattern 
			for (int i = j; i < session.size(); i++) {
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
				}
			}
		}
	}
}
