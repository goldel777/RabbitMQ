package com.golden.rabbitmq.client;

import java.io.IOException;
import java.util.Random;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


//import static com.golden.rabbitmq.client.RabbitClientSend.QUEUE_NAME;
// something changed on git commit xtra

/* test change xxx yyy */
public class RabbitClientReceive {
	public final static String QUEUE_NAME = "durable_hello2";
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	public static void main(String[] args) throws IOException, InterruptedException{
		RabbitClientReceive receiver = new RabbitClientReceive();
		
		receiver.initialize();
		receiver.receive();
				
		receiver.close();
	}
	
	public void initialize() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		
		int prefetchCount = 1;
		
		channel.basicQos(prefetchCount);
		
	}

	public void receive() throws IOException,InterruptedException {
		// declared twice, then either sender or receiver can start first
		boolean durable = true;
		 channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
		 System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		 
		 
		 // QueuingConsumer buffers the input
		 QueueingConsumer consumer = new QueueingConsumer(channel);
		 // basicConsume(java.lang.String queue, boolean autoAck, Consumer callback)
		 boolean autoAck = false;
		 channel.basicConsume(QUEUE_NAME, autoAck, consumer);
		 
		 Random random = new Random();
		 
		 while (true) {
			 QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			 String message = new String(delivery.getBody());
			 boolean multiple = false;
			 boolean requeue = true;
			 
			 System.out.println(" [x] Received '" + message + "'");
			 
			 doWork(message);
			 //rabbitmqctl to look for unacked messages
			 switch (random.nextInt(2)) {
			 case 0:
			     channel.basicNack(delivery.getEnvelope().getDeliveryTag(), multiple, requeue);
			     System.out.println(" [x] Rejected '" + message + "'");
			     break;
			 case 1:
				 channel.basicAck(delivery.getEnvelope().getDeliveryTag(),multiple);
				 System.out.println(" [x] Processed '" + message + "'");
				 break;
			 case 2:  break;
				 
			 }
		 
		 }
	}
	
	public void doWork(String message) {
		try {
			for (char c: message.toCharArray()) {
				if (c == '.') {
					Thread.sleep(1000);
				}
			}
		} catch(InterruptedException ex) {}
	}
	
	public void close() throws IOException {
		channel.close();
		connection.close();
	}
}
