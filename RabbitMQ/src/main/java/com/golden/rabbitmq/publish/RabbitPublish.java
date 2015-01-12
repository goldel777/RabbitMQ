package com.golden.rabbitmq.publish;



import java.io.IOException;
import java.util.Scanner;

import com.rabbitmq.client.AMQP;
//import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/*
 * Essentially, published log messages are going to be broadcast to all the receivers.
 * 
 * 
    A producer is a user application that sends messages.
    A queue is a buffer that stores messages.
    A consumer is a user application that receives messages.

The core idea in the messaging model in RabbitMQ is that the producer never
 sends any messages directly to a queue. Actually, quite often the producer
  doesn't even know if a message will be delivered to any queue at all.

Instead, the producer can only send messages to an exchange. An exchange is 
a very simple thing. On one side it receives messages from producers and 
the other side it pushes them to queues. The exchange must know exactly
 what to do with a message it receives. Should it be appended to a 
 particular queue? Should it be appended to many queues? Or should it
  get discarded. The rules for that are defined by the exchange type.
  
  message queue -> append to 1 queue
  publish subscribe -> exchange puts on many queues
  
  types of exchanges:
  
  direct, topic, headers and fanout
  
  The fanout exchange is very simple. As you can probably guess from
   the name, it just broadcasts all the messages it receives to 
   all the queues it knows. And that's exactly what we need for our logger.
   
   Firstly, whenever we connect to Rabbit we need a fresh, empty queue. 
   To do this we could create a queue with a random name, or, even better 
   - let the server choose a random queue name for us.

Secondly, once we disconnect the consumer the queue should be automatically
 deleted.

In the Java client, when we supply no parameters to queueDeclare() we 
create a non-durable, exclusive, autodelete queue with a generated name:

String queueName = channel.queueDeclare().getQueue();

channel.queueBind(queueName, "logs", "");

 */
public class RabbitPublish {
	public final static String EXCHANGE_NAME="logs";
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	public static void main(String[] args) throws IOException {
		RabbitPublish subscribe = new RabbitPublish();
		
		subscribe.initialize();
		subscribe.send();
		subscribe.close();
		
	}
	
	
	private void initialize() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		
	}
	
	private void send() throws IOException {
		Scanner scanner = new Scanner(System.in);
		String input;
		
		while ((input = scanner.nextLine()) != null) {
			String routingKey = "";
			AMQP.BasicProperties props =null;
			
			channel.basicPublish(EXCHANGE_NAME, routingKey, props, input.getBytes());
			System.out.printf("Message Sent %s", input);
			
		}
		
		scanner.close();
	}
	
	private void close() throws IOException {
		channel.close();
		connection.close();
	}

}
