package com.golden.rabbitmq.client;


import java.io.IOException;
import java.util.Scanner;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

/*
 * In the previous tutorial we created a work queue. 
 * The assumption behind a work queue is that each task is 
 * delivered to exactly one worker.
 */
public class RabbitClientSend {
	//public final static String QUEUE_NAME = "hello";
	public final static String QUEUE_NAME = "durable_hello2";

	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;

	public RabbitClientSend() {
	}

	// Set up the class and name the queue:

	public static void main(String[] argv) throws java.io.IOException {
		RabbitClientSend client = new RabbitClientSend();
		
		try (Scanner scanner = new Scanner(System.in)) {
			String str;
			
			client.initialize();
			
			while ((str = scanner.nextLine()).length() > 0) {
				System.out.printf("Sent::%s", str);
				client.send(str);
			}
		}
		System.out.println("Send exit");
		client.close();

	}

	/*
	 * The connection abstracts the socket connection, and takes care of
	 * protocol version negotiation and authentication and so on for us. Here we
	 * connect to a broker on the local machine - hence the localhost. If we
	 * wanted to connect to a broker on a different machine we'd simply specify
	 * its name or IP address here.
	 * 
	 * ConnectionFactory factory = new ConnectionFactory();
	 * factory.setUsername(userName); factory.setPassword(password);
	 * factory.setVirtualHost(virtualHost); factory.setHost(hostName);
	 * factory.setPort(portNumber);
	 * 
	 * factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost")
	 * ;
	 * 
	 * ExecutorService es = Executors.newFixedThreadPool(20);
	 * 
	 * Connection conn = factory.newConnection(es);
	 * 
	 * 
	 * Next we create a channel to send/receive messages, which is where most of
	 * the API for getting things done resides.
	 */

	private void initialize() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost("localhost");

		connection = factory.newConnection();
		channel = connection.createChannel();
		
		
		
		/*
		 * so it will just give messages to the next consumer
		 * even if the consumer has unacked messages. 
		 * Not sure if this means sync via the ConsumerQueue?
		 * 
		 * In order to defeat that we can use the basicQos method with
		 * the prefetchCount = 1 setting. This tells RabbitMQ not 
         * give more than one message to a worker at a time. 
         * Or, in other words, don't dispatch a new message to a worker 
         * until it has processed and acknowledged the previous one. 
         * Instead, it will dispatch it to the next worker that is not 
         * still busy.
         * 
         * Note about queue size
         * 
         * If all the workers are busy, your queue can fill up. 
         * You will want to keep an eye on that, and maybe add more
         *  workers, or have some other strategy.
		 */
	}

	// To send, we must declare a queue for us to send to; then we can publish a
	// message to the queue:
	//
	// Declaring a queue is idempotent - it will only be created if it doesn't
	// exist already.
	// The message content is a byte array, so you can encode whatever you like
	// there.

	/*
	 * Although this command is correct by itself, it won't work in 
	 * our present setup. That's because we've already defined a
	 *  queue called hello which is not durable. RabbitMQ doesn't 
	 *  allow you to redefine an existing queue with different 
	 *  parameters and will return an error to any program that 
	 *  tries to do that. But there is a quick workaround - 
	 *  let's declare a queue with different name, for example
	 *  task_queue:
	 *  
	 *  Marking messages as persistent doesn't fully guarantee that a
	 *   message won't be lost. Although it tells RabbitMQ to save the 
	 *   message to disk, there is still a short time window when RabbitMQ
	 *    has accepted a message and hasn't saved it yet. Also, RabbitMQ 
	 *    doesn't do fsync(2) for every message -- it may be just saved to 
	 *    cache and not really written to the disk. The persistence 
	 *    guarantees aren't strong, but it's more than enough for our 
	 *    simple task queue. If you need a stronger guarantee then you can 
	 *    use publisher confirms.
	 */
	private void send(String message) throws IOException {
		// AMQP.Queue.DeclareOk queueDeclare(java.lang.String queue, boolean
		// durable, boolean exclusive, boolean autoDelete,
		// java.util.Map<java.lang.String,java.lang.Object> arguments)
		boolean durable = true;
		boolean exclusive = false; // think this all can't be changed once queue is created
		boolean autoDelete = false; // certainly true for durable

		// declared twice (idempotent), so that either sender or reciver can
		// call it first
		channel.queueDeclare(QUEUE_NAME, durable, exclusive, autoDelete, null);

		// void basicPublish(java.lang.String exchange, java.lang.String
		// routingKey, AMQP.BasicProperties props, byte[] body)
		channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
		System.out.println(" [x] Sent '" + message + "'");

	}

	// Lastly, we close the channel and the connection;
	private void close() throws IOException {

		channel.close();
		connection.close();
	}

}
