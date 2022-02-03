package org.trishinfotech.activemq.example1;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class SimpleQueue {

	private static final String CLIENTID = "TrishInfotechActiveMQ";
	private String queueName;
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageProducer producer;
	private MessageConsumer consumer;
	
	public SimpleQueue(String queueName) throws Exception {
		super();
		// The name of the queue.
		this.queueName = queueName;
		// URL of the JMS server is required to create connection factory.
		// DEFAULT_BROKER_URL is : tcp://localhost:61616 and is indicates that JMS
		// server is running on localhost
		connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
		// Getting JMS connection from the server and starting it
		connection = connectionFactory.createConnection();
		connection.setClientID(CLIENTID);
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
	}

	public void send(String textMessage)
			throws Exception {
		// We will send a text message 
		TextMessage message = session.createTextMessage(textMessage);
		// push the message into queue
		producer.send(message);
		System.out.printf("'%s' text message sent to the queue '%s' running on local JMS Server.\n", message, queueName);
	}
	
	public void receive() throws Exception {
		// receive the message from the queue.
		Message message = consumer.receive();
		// Since We are using TestMessage in our example. MessageProducer sent us a TextMessage
		// So we need cast to it to get access to its getText() method which will give us the text of the message
		if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			System.out.printf("Received message '%s' from the queue '%s' running on local JMS Server.\n", textMessage.getText(), queueName);
		}
	}

	public void close() throws JMSException {
		producer.close();
		producer = null;
		consumer.close();
		consumer = null;
		session.close();
		session = null;
		connection.close();
		connection = null;
	}
}