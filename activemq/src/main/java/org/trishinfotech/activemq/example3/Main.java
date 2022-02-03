package org.trishinfotech.activemq.example3;

import java.util.Random;

import org.trishinfotech.activemq.example2.BankAccount;
import org.trishinfotech.activemq.example2.MyQueue;

public class Main {

	private static final String QUEUE_NAME = "BankAccountProcessingQueue";
	private static final int NO_OF_CONSUMERS = 2;
	private static final long NO_OF_ACCOUNTS = 100l;

	public static void main(String[] args) throws Exception {
		MyQueue queue = new MyQueue(QUEUE_NAME, NO_OF_CONSUMERS);
		Random rand = new Random();
		System.out.printf("%10s | %10s | %10s | %50s\n", "Source", "Action", "Result",
				"Bank Details (ApplicationNo,  UserName, DepositAmount, CustomerId, ATM)");
		System.out.println(
				"=================================================================================================================");
		for (long i = 1; i <= NO_OF_ACCOUNTS; i++) {
			long applicationNo = rand.nextLong(NO_OF_ACCOUNTS);
			BankAccount newAccount = new BankAccount(applicationNo, "Customer" + applicationNo, 1000.0d);
			queue.sendAccountToQueue(queue.getProducer(), newAccount);
		}
		System.out.println(
				"=================================================================================================================");
		// just to give graceful time to finish the processing
		Thread.sleep(2000);
		queue.printSummary();
		queue.close();
	}

}
