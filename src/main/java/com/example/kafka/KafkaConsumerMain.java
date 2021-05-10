package com.example.kafka;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.example.kafka.util.ConfigUtil;

public class KafkaConsumerMain {

	public static void main(String[] args) {

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(ConfigUtil.getKafkaConsumerConfig());
		kafkaConsumer.subscribe(Collections.singletonList("products"));

		Thread mainThread = Thread.currentThread();
		Runnable r = () -> {
			// wakeup is the only method we can call from another method to stop the
			// polling.
			kafkaConsumer.wakeup();
			try {
				mainThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread(r));

		try {

			while (true) {
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : consumerRecords) {

					System.out.println(record.key());
					System.out.println(record.value());
				}

			}

		} catch (WakeupException w) {
			System.out.println("Inside wakeup..." + w.getMessage());
		} finally {
			//Closing the consumer is must.
			kafkaConsumer.close();
		}

	}

}
