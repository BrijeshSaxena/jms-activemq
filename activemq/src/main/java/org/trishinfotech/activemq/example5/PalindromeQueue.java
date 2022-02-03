package org.trishinfotech.activemq.example5;

import javax.jms.Message;
import javax.jms.ObjectMessage;

public class PalindromeQueue extends MyQueue {

	public PalindromeQueue(String calculateName, String queueName) throws Exception {
		super(calculateName, queueName);
	}

	@Override
	public String processCalculationWorkFromQueue(Message message, String consumerTaskName) throws Exception {
		String answer = "false";
		CalculationWork calculationWork = null;
		if (message != null) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			calculationWork = (CalculationWork) objectMessage.getObject();
			if (calculationWork != null) {
				String value = calculationWork.getValue();
				if (value != null && !value.trim().isEmpty()) {
					String reverse = (new StringBuilder(value).reverse().toString());
					answer = Boolean.toString(reverse.equals(value));
				}
			}
			message.acknowledge();
		}
		return answer;
	}

}