package com.golden.rabbitmq.client;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import static com.golden.rabbitmq.publish.RabbitPublish.EXCHANGE_NAME;


public class RabbitSubscribe {
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	//private Queue   queue;
	private String queueName;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		RabbitSubscribe subscribe = new RabbitSubscribe();
		
		subscribe.initialize();
		subscribe.send();
		subscribe.close();
			
	}
	
	private void initialize() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		
		queueName = channel.queueDeclare().getQueue();
		
		String routingKey = "";
		channel.queueBind(queueName,  EXCHANGE_NAME, routingKey);
	}
	
	
	private void send() throws IOException, InterruptedException {
		boolean autoAck = true;
		QueueingConsumer consumer = new QueueingConsumer(channel);
		
		channel.basicConsume(queueName, autoAck, consumer);
		
		while (true) {
			Delivery delivery = consumer.nextDelivery();
			String msg = new String(delivery.getBody());
			
			System.out.println("LOG MSG::"+msg);
			
			
			
		}
	
	}
	
	private void close() throws IOException {
		channel.close();
		connection.close();
		
	}
	
	
	
}