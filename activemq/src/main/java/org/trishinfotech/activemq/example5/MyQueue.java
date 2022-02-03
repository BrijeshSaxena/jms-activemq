package org.trishinfotech.activemq.example5;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class MyQueue {

	protected String calculateName;
	protected String queueName;
	protected ConnectionFactory connectionFactory;
	protected Connection connection;
	protected Session session;
	protected Destination destination;
	protected MessageProducer producer;
	protected MessageConsumer consumer;
	protected int noOfSend = 0;

	public MyQueue(String calculateName, String queueName) throws Exception {
		super();
		this.calculateName = calculateName;
		// The name of the queue.
		this.queueName = queueName;
		// URL of the JMS server is required to create connection factory.
		// DEFAULT_BROKER_URL is : tcp://localhost:61616 and is indicates that JMS
		// server is running on localhost
		connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
		// Getting JMS connection from the server and starting it
		connection = connectionFactory.createConnection("admin", "admin");
		connection.start();
		// Creating a non-transactional session to send/receive JMS message.
		session = connection.createSession(false, AUTO_ACKNOWLEDGE);
		// Destination represents here our queue ’BankAccountProcessingQueue’ on the JMS
		// server.
		// The queue will be created automatically on the JSM server if its not already
		// created.
		destination = session.createQueue(this.queueName);
		// MessageProducer is used for sending (producing) messages to the queue.
		producer = session.createProducer(destination);
		// MessageConsumer is used for receiving (consuming) messages from the queue.
		consumer = session.createConsumer(destination);
		consumer.setMessageListener(new MyQueueListener(this, queueName + "Consumer"));
	}

	public void close() throws JMSException {
		producer.close();
		producer = null;
		consumer.close();
		session.close();
		session = null;
		connection.close();
		connection = null;
	}
	
	public void sendCalculationWorkToQueue(CalculationWork calculationWork) throws Exception {
		System.out.printf("%40s | %10s | %-50s\n", queueName + "Producer", "Sending", calculationWork);
		ObjectMessage message = session.createObjectMessage(calculationWork);
		// push the message into queue
		producer.send(message);
		noOfSend++;
	}

	public String getCalculateName() {
		return calculateName;
	}

	public String getQueueName() {
		return queueName;
	}

	public Connection getConnection() {
		return connection;
	}

	public Session getSession() {
		return session;
	}

	public Destination getDestination() {
		return destination;
	}

	public MessageProducer getProducer() {
		return producer;
	}
	
	public abstract String processCalculationWorkFromQueue(Message message, String consumerTaskName) throws Exception;

	private static class MyQueueListener implements MessageListener {

		private MyQueue queue;
		private String consumerTaskName;

		public MyQueueListener(final MyQueue queue, String consumerTaskName) {
			super();
			this.queue = queue;
			this.consumerTaskName = consumerTaskName;
		}

		@Override
		public void onMessage(Message message) {
			// on poll the message from the queue
			try {
				ObjectMessage objectMessage = (ObjectMessage) message;
				CalculationWork calculationWork = (CalculationWork) objectMessage.getObject();
				String answer = queue.processCalculationWorkFromQueue(message, consumerTaskName);
				System.out.printf("%40s | %10s | %-50s \n", consumerTaskName, queue.getCalculateName(), calculationWork + " : " +answer);
			} catch (Exception exp) {
				// we can ignore as of now
			}

		}

	}

}
