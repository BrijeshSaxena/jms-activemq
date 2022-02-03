package org.trishinfotech.activemq.example1;

public class ConsumerMain {

	private static final String QUEUE_NAME = "MyFirstActiveMQ";

	public static void main(String[] args) throws Exception {
		SimpleQueue queue = new SimpleQueue(QUEUE_NAME);
		queue.receive();
		queue.close();
	}

}
