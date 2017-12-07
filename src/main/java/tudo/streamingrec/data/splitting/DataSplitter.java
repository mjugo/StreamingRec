package tudo.streamingrec.data.splitting;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import tudo.streamingrec.data.Event;
import tudo.streamingrec.data.RawData;
import tudo.streamingrec.data.SplitData;

/**
 * A class to sort the data (items and clicks) and split it into training and test set.
 * @author Mozhgan
 *
 */
public class DataSplitter {
	
	//The split threshold between training and test set (0.1 = 10% training, 90% testing)
	private double threshold;
	//Should the split be time based (e.g. 30% of the time for training) or 
	//based on the amount of actions.
	private boolean timeBasedSplit;

	/**
	 * The split threshold between training and test set (0.1 = 10% training, 90% testing)
	 * @param threshold -
	 */
	public void setSplitThreshold(double threshold){
		this.threshold = threshold;		
	}
	
	/**
	 * Should the split be time based (e.g. 30% of the time for training)?
	 */
	public void setSplitMethodTime(){
		this.timeBasedSplit = true;
	}
	
	/**
	 * Should the split be based on the number of events  (e.g. 30% of the total amount of events for training)?
	 */
	public void setSplitMethodNumberOfEvents(){
		this.timeBasedSplit = false;
	}

	/**
	 * Splits the data (items and transactions) into training and test set 
	 * based on the paramters defined through {@link #setSplitThreshold(double)},
	 * {@link #setSplitMethodTime()}, and {@link #setSplitMethodNumberOfEvents()}.
	 * @param data -
	 * @return the split data
	 */
	public SplitData splitData(RawData data){
		//Add all events (items and transactions) to one list
		List<Event> allEvents = new ObjectArrayList<Event>();
		allEvents.addAll(data.items.values());
		allEvents.addAll(data.transactions);
		
		//Sort based on the time
		Collections.sort(allEvents, new Comparator<Event>(){
			public int compare(Event o1, Event o2) {
				return o1.getEventTime().compareTo(o2.getEventTime());
			}			
		});
		
		//create traing and test set
		List<Event> trainingSet = new ObjectArrayList<Event>();
		List<Event> testSet = new ObjectArrayList<Event>();
		
		//depending on the parameterization, either split based on time or nb. of events.
		if(timeBasedSplit){
			//Find the right cut-off time
			Date startTime = allEvents.get(0).getEventTime();
			Date endTime = allEvents.get(allEvents.size()-1).getEventTime();
			Date cutOffTime = new Date(startTime.getTime() + (long)((endTime.getTime()-startTime.getTime())*threshold));
			
			//iterate to find the first event above the cut-off time and split
			for (int i = 0; i < allEvents.size(); i++) {
				Event event = allEvents.get(i);
				if(event.getEventTime().before(cutOffTime)){
					trainingSet.add(event);
				}else{
					testSet.add(event);
				}				
			}
		}else{
			//find the right item to cut off at.
			int cutOff = (int) (allEvents.size()*threshold);
			trainingSet.addAll(allEvents.subList(0, cutOff));
			testSet.addAll(allEvents.subList(cutOff, allEvents.size()));
		}
		
		//return the splitted set
		SplitData splitData = new SplitData();
		splitData.trainingData = trainingSet;
		splitData.testData = testSet;
		return splitData;
	}
}
