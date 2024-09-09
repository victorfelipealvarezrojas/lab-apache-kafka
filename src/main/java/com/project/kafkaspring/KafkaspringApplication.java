package com.project.kafkaspring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class KafkaspringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(
			topics = "devs4j-topic",
			groupId = "devs4j-group",
			containerFactory = "listenerContainerFactory",
			properties = {
					"max.poll.interval.ms=600000", // es el tiempo máximo que un consumidor puede estar sin enviar un mensaje al grupo de consumidores
					"max.poll.records=100", // es el número máximo de registros que un consumidor puede obtener en una sola llamada a poll
			}
	)
	public void Listen(List<String> messages) {
		messages.forEach(
				message -> System.out.println("Message: " + message)
		);
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaspringApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Hello from Spring Kafka!");
		future.whenComplete((result, ex) -> {
			if (ex == null) {
				System.out.println("Message sent successfully::" + result.getRecordMetadata().offset());
			} else {
				System.out.println("Error sending message::" + ex.getMessage());
			}
		});
	}
}
