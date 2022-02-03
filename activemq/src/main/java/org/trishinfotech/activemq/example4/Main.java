package org.trishinfotech.activemq.example4;

import java.util.Random;

import org.trishinfotech.activemq.example2.BankAccount;

public class Main {

	private static final String TOPIC_NAME = "BankAccountProcessingTopic";
	private static final int NO_OF_CONSUMERS = 2;
	private static final long NO_OF_ACCOUNTS = 100l;

	public static void main(String[] args) throws Exception {
		MyTopic topic = new MyTopic(TOPIC_NAME, NO_OF_CONSUMERS);
		Random rand = new Random();
		System.out.printf("%10s | %10s | %10s | %50s\n", "Source", "Action", "Result",
				"Bank Details (ApplicationNo,  UserName, DepositAmount, CustomerId, ATM)");
		System.out.println(
				"=================================================================================================================");
		for (long i = 1; i <= NO_OF_ACCOUNTS; i++) {
			long applicationNo = rand.nextLong(NO_OF_ACCOUNTS);
			BankAccount newAccount = new BankAccount(applicationNo, "Customer" + applicationNo, 1000.0d);
			topic.sendAccountToTopic(topic.getProducer(), newAccount);
		}
		System.out.println(
				"=================================================================================================================");
		// just to give graceful time to finish the processing
		Thread.sleep(2000);
		topic.printSummary();
		topic.close();
	}

}
