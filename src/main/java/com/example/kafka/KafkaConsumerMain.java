package com.example.kafka;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.example.kafka.util.ConfigUtil;

public class KafkaConsumerMain {

	public static void main(String[] args) {
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(ConfigUtil.getKafkaConsumerConfig());
		kafkaConsumer.subscribe(Collections.singletonList("products"));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : consumerRecords) {
				
				System.out.println(record.key());
				System.out.println(record.value());
			}
			
		}

	}

}
