package com.project.kafkaspring;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class KafkaspringApplication {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private MeterRegistry meterRegistry;

	@KafkaListener(
			id = "devs4j-listener",
			autoStartup = "true", // si se establece en false, el contenedor no se iniciará automáticamente
			topics = "devs4j-topic",
			groupId = "devs4j-group",
			containerFactory = "listenerContainerFactory",
			properties = {
					"max.poll.interval.ms=600000", // es el tiempo máximo que un consumidor puede estar sin enviar un mensaje al grupo de consumidores
					"max.poll.records=50", // es el número máximo de registros que un consumidor puede obtener en una sola llamada a poll
			}
	)

	public void Listen(List<ConsumerRecord<String, String>> messages) throws InterruptedException {
		messages.forEach(
				message -> System.out.println("Message: " + message + " offSer: " + message.offset())
		);
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaspringApplication.class, args);
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 1000)
	public void sendKafkaMessages() {
		for (int i = 0; i < 100; i++) {
			CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
					"devs4j-topic",
					"Message " + i
			);

			future.whenComplete((result, ex) -> {
				if (ex == null) {
					System.out.println(
							" Message sent: " + result.getProducerRecord().value() +
									" OffSet: " + result.getRecordMetadata().offset() +
									" partition: " + result.getRecordMetadata().partition() +
									" topic: " + result.getRecordMetadata().topic()
					);
				} else {
					System.out.println("Error sending message");
				}
			});
		}
	}
}