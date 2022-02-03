package org.trishinfotech.activemq.example5;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

public class MyQueueManager {

	protected Map<String, MyQueue> queueMap = new HashMap<String, MyQueue>();

	public void manageQueue(MyQueue myQueue) {
		queueMap.put(myQueue.getCalculateName(), myQueue);
	}

	public void close() {
		queueMap.values().forEach(myQueue -> {
			try {
				myQueue.close();
			} catch (JMSException exp) {
				// we can ignore as of now
			}
		});
	}

	public void sendToQueue(CalculationWork calculationWork) throws Exception {
		MyQueue myQueue = findQueue(calculationWork);
		if (myQueue != null) {
			myQueue.sendCalculationWorkToQueue(calculationWork);
		}
	}

	public MyQueue findQueue(CalculationWork calculationWork) {
		return queueMap.get(calculationWork.getCalculateName());
	}
	
}