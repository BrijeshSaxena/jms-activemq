package org.trishinfotech.activemq.example4;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.trishinfotech.activemq.example2.BankAccount;

public class MyTopic {

	private static final String CLIENTID = "TrishInfotechActiveMQ";
	private String topicName;
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Topic topic;
	private MessageProducer producer;
	private List<MessageConsumer> consumers = new ArrayList<MessageConsumer>();
	private Map<Long, BankAccount> accountMap = new HashMap<Long, BankAccount>();
	private int noOfSend = 0;

	public MyTopic(String topicName, int noOfConsumers) throws Exception {
		super();
		// The name of the topic.
		this.topicName = topicName;
		// URL of the JMS server is required to create connection factory.
		// DEFAULT_BROKER_URL is : tcp://localhost:61616 and is indicates that JMS
		// server is running on localhost
		connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
		// Getting JMS connection from the server and starting it
		connection = connectionFactory.createConnection("admin", "admin");
		connection.setClientID(CLIENTID);
		connection.start();
		// Creating a non-transactional session to send/receive JMS message.
		session = connection.createSession(false, AUTO_ACKNOWLEDGE);
		// Topic represents here our Topic ’BankAccountProcessingTopic’ on the JMS
		// server.
		// The Topic will be created automatically on the JSM server if its not already
		// created.
		topic = session.createTopic(this.topicName);
		// MessageProducer is used for sending (producing) messages to the Topic.
		producer = session.createProducer(topic);
		// MessageConsumer is used for receiving (consuming) messages from the Topic.
		IntStream.range(0, noOfConsumers).forEach(consumerNo -> {
			try {
				MessageConsumer consumer = session.createConsumer(topic);
				consumer.setMessageListener(new MyTopicListener(this, "Consumer" + (consumerNo + 1)));
				consumers.add(consumer);
			} catch (Exception exp) {
				// we can ignore as of now
			}
		});
	}

	public void sendAccountToTopic(MessageProducer producer, BankAccount newAccount) throws Exception {
		System.out.printf("%10s | %10s | %10s | %50s\n", "Producer", "Sending", "-", newAccount);
		ObjectMessage message = session.createObjectMessage(newAccount);
		// push the message into Topic
		producer.send(message);
		noOfSend++;
	}

	public ProcessStatus processAccountFromTopic(Message message, String consumerTaskName) throws Exception {
		ProcessStatus status = ProcessStatus.TOPIC_CLOSED;
		BankAccount account = null;
		if (message != null) {
			ObjectMessage objectMessage = (ObjectMessage) message;
			account = (BankAccount) objectMessage.getObject();
			if (account != null) {
				BankAccount existingAccount = pullAccountByApplicationNo(account.getApplicationNo());
				if (existingAccount != null) {
					System.out.printf("%10s | %10s | %10s | %50s\n", "Consumer", consumerTaskName, "Existing",
							existingAccount);
					account = existingAccount;
					status = ProcessStatus.EXISTING;
				} else {
					createBankAccount(account);
					System.out.printf("%10s | %10s | %10s | %50s\n", "Consumer", consumerTaskName, "Created", account);
					status = ProcessStatus.CREATED;
				}
			}
			//message.acknowledge();
		}
		return status;
	}

	private void createBankAccount(BankAccount account) throws Exception {
		Helper.setupBankAccount(account);
		accountMap.put(account.getApplicationNo(), account);
	}

	public BankAccount pullAccountByApplicationNo(long applicationNo) {
		return accountMap.get(applicationNo);
	}

	public void close() throws JMSException {
		producer.close();
		producer = null;
		consumers.stream().forEach(consumer -> {
			try {
				consumer.close();
			} catch (JMSException e) {
				// we can ignore as of now
			}
		});
		consumers.clear();
		session.close();
		session = null;
		connection.close();
		connection = null;
	}

	public String getTopicName() {
		return topicName;
	}

	public Connection getConnection() {
		return connection;
	}

	public Session getSession() {
		return session;
	}

	public Topic getTopic() {
		return topic;
	}

	public MessageProducer getProducer() {
		return producer;
	}

	public void printSummary() {
		System.out.printf("\n\n%30s | %14s | %14s | %14s\n", "Source", "No Of Created", "No Of Existing",
				"No Of Processed");
		System.out.println("====================================================================================");
		AtomicInteger totalNoOfExisting = new AtomicInteger();
		consumers.stream().forEach(consumer -> {
			try {
				MyTopicListener listener = (MyTopicListener) consumer.getMessageListener();
				listener.printSummary();
				totalNoOfExisting.getAndAdd(listener.getNoOfExisting());
			} catch (JMSException exp) {
				// we can ignore as of now
				exp.printStackTrace();
			}
		});
		System.out.println("------------------------------------------------------------------------------------");
		System.out.printf("%30s | %14s | %14s | %14s\n", topicName, accountMap.size(), totalNoOfExisting.get(),
				noOfSend);
		System.out.println("====================================================================================");
	}

	private static enum ProcessStatus {
		CREATED, EXISTING, TOPIC_CLOSED
	}

	private static class Helper {
		private static long initialCustomerId = 1234567l;
		private static long accountCreated = 0l;

		public static void setupBankAccount(BankAccount account) {
			long nextAvailableCustomerId = initialCustomerId + accountCreated++;
			Random rand = new Random();
			String atmCardNumber = String.format("%04d %04d %04d %04d", rand.nextInt(9999), rand.nextInt(9999),
					rand.nextInt(9999), rand.nextInt(9999));
			account.setCustomerId(nextAvailableCustomerId);
			account.setAtmCardNumber(atmCardNumber);
		}
	}

	private static class MyTopicListener implements MessageListener {

		private MyTopic Topic;
		private String consumerTaskName;
		private int noOfProcessed = 0, noOfCreated = 0, noOfExisting = 0;

		public MyTopicListener(final MyTopic Topic, String consumerTaskName) {
			super();
			this.Topic = Topic;
			this.consumerTaskName = consumerTaskName;
		}

		@Override
		public void onMessage(Message message) {
			// on poll the message from the Topic
			try {
				ProcessStatus status = Topic.processAccountFromTopic(message, consumerTaskName);
				switch (status) {
				case CREATED:
					noOfCreated++;
					break;
				case EXISTING:
					noOfExisting++;
					break;
				case TOPIC_CLOSED:
				default:
				}
				if (!ProcessStatus.TOPIC_CLOSED.equals(status)) {
					noOfProcessed++;
				}
			} catch (Exception exp) {
				// we can ignore as of now
			}

		}

		public int getNoOfProcessed() {
			return noOfProcessed;
		}

		public int getNoOfCreated() {
			return noOfCreated;
		}

		public int getNoOfExisting() {
			return noOfExisting;
		}

		public void printSummary() {
			System.out.printf("%30s | %14s | %14s | %14s\n", consumerTaskName, getNoOfCreated(), getNoOfExisting(),
					getNoOfProcessed());
		}

	}
}