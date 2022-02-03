package org.trishinfotech.activemq.example2;

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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class MyQueueSynchronized {

	private static final String CLIENTID = "TrishInfotechActiveMQ";
	private String queueName;
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageProducer producer;
	private Map<Long, BankAccount> accountMap = new HashMap<Long, BankAccount>();
	private List<QueueConsumerTask> tasks;
	private int noOfSend = 0;
	
	public MyQueueSynchronized(String queueName, int noOfConsumers) throws Exception {
		super();
		// The name of the queue.
		this.queueName = queueName;
		// URL of the JMS server is required to create connection factory.
		// DEFAULT_BROKER_URL is : tcp://localhost:61616 and is indicates that JMS
		// server is running on localhost
		this.connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
		// Getting JMS connection from the server and starting it
		this.connection = connectionFactory.createConnection();
		connection.setClientID(CLIENTID);
		this.connection.start();
		// Creating a non-transactional session to send/receive JMS message.
		this.session = this.connection.createSession(false, AUTO_ACKNOWLEDGE);
		// Destination represents here our queue ’BankAccountProcessingQueue’ on the JMS
		// server.
		// The queue will be created automatically on the JSM server if its not already
		// created.
		this.destination = session.createQueue(this.queueName);
		// MessageProducer is used for sending (producing) messages to the queue.
		this.producer = session.createProducer(destination);
		// MessageConsumer is used for receiving (consuming) messages from the queue.
		tasks = new ArrayList<QueueConsumerTask>();
		IntStream.range(0, noOfConsumers).forEach(consumerNo -> {
			try {
				MessageConsumer consumer = session.createConsumer(destination);
				QueueConsumerTask task = new QueueConsumerTask(this, consumer, "Consumer" + (consumerNo + 1));
				tasks.add(task);
			} catch (Exception exp) {
				// we can ignore as of now
			}
		});
		tasks.stream().forEach(task -> new Thread(task).start());
	}

	public void sendAccountToQueue(MessageProducer producer, BankAccount newAccount)
			throws Exception {
		System.out.printf("%10s | %10s | %10s | %50s\n", "Producer", "Sending", "-", newAccount);
		ObjectMessage message = session.createObjectMessage(newAccount);
		// push the message into queue
		producer.send(message);
		noOfSend++;
	}
	
	public ProcessStatus processAccountFromQueue(MessageConsumer consumer, String consumerTaskName) throws Exception {
		ProcessStatus status = ProcessStatus.QUEUE_CLOSED;
		// poll the message from the queue
		Message message = consumer.receive();
		BankAccount account = null;
		if (message != null) {
			ObjectMessage textMessage = (ObjectMessage) message;
			account = (BankAccount) textMessage.getObject();
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
			message.acknowledge();
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
		tasks.stream().forEach(task -> task.stop());
		tasks.clear();
		session.close();
		session = null;
		connection.close();
		connection = null;
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

	public void printSummary() {
		System.out.printf("\n\n%30s | %14s | %14s | %14s\n", "Source", "No Of Created", "No Of Existing",
				"No Of Processed");
		System.out.println("====================================================================================");
		AtomicInteger totalNoOfExisting = new AtomicInteger();
		tasks.stream().forEach(task -> {
			task.printSummary();
			totalNoOfExisting.getAndAdd(task.getNoOfExisting());
		});
		System.out.println("------------------------------------------------------------------------------------");
		System.out.printf("%30s | %14s | %14s | %14s\n", queueName, accountMap.size(), totalNoOfExisting.get(), noOfSend);
		System.out.println("====================================================================================");
	}

	private static enum ProcessStatus {
		CREATED, EXISTING, QUEUE_CLOSED
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

	private static class QueueConsumerTask implements Runnable {

		private MyQueueSynchronized queue;
		private MessageConsumer consumer;
		private String consumerTaskName;
		private boolean keepRunning = true;
		private int noOfProcessed = 0, noOfCreated = 0, noOfExisting = 0;

		public QueueConsumerTask(final MyQueueSynchronized queue, MessageConsumer consumer, String consumerTaskName) {
			super();
			this.queue = queue;
			this.consumer = consumer;
			this.consumerTaskName = consumerTaskName;
		}

		public void stop() {
			this.keepRunning = false;
		}

		@Override
		public void run() {
			while (keepRunning) {
				try {
					ProcessStatus status = queue.processAccountFromQueue(consumer, consumerTaskName);
					switch (status) {
					case CREATED:
						noOfCreated++;
						break;
					case EXISTING:
						noOfExisting++;
						break;
					case QUEUE_CLOSED:
					default:
					}
					if (!ProcessStatus.QUEUE_CLOSED.equals(status)) {
						noOfProcessed++;
					}
				} catch (Exception exp) {
					exp.printStackTrace();
				}
				finally {
					try {
						consumer.close();
						consumer = null;
					} catch (JMSException e) {
						// we can ignore as of now
					}
				}
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