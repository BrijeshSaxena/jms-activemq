package org.trishinfotech.activemq.example5;

import java.io.BufferedReader;
import java.io.FileReader;

public class Main {

	private static final String QUEUE_ARMSTRONG = "ArmstrongCalculationQueue";
	private static final String QUEUE_FACTORIAL = "FactorialCalculationQueue";
	private static final String QUEUE_PALINDROME = "PalindromeCalculationQueue";
	
	public static void main(String[] args) throws Exception {
		MyQueueManager queueManager = new MyQueueManager();
		queueManager.manageQueue(new ArmstrongQueue("Armstrong", QUEUE_ARMSTRONG));
		queueManager.manageQueue(new FactorialQueue("Factorial", QUEUE_FACTORIAL));
		queueManager.manageQueue(new PalindromeQueue("Palindrome", QUEUE_PALINDROME));
		
		System.out.printf("%40s | %10s | %-50s\n", "Source", "Action", "Result/Details");
		System.out.println(
				"=================================================================================================================");
		BufferedReader reader = new BufferedReader(new FileReader("./CalculationWork.txt"));
		String line;
        while ((line = reader.readLine()) != null) {
        	CalculationWork calculationWork = CalculationWork.valueOf(line);
        	if (calculationWork != null) {
        		queueManager.sendToQueue(calculationWork);
        	}
        }
        reader.close();
		// just to give graceful time to finish the processing
		Thread.sleep(2000);
		queueManager.close();
		System.out.println(
				"=================================================================================================================");
	}

}
