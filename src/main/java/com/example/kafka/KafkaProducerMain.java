package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.example.kafka.util.ConfigUtil;

public class KafkaProducerMain {

	public static void main(String[] args) {
				
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(ConfigUtil.getKafkaProducerConfig());
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("products", "key-1","Bose");
		
		kafkaProducer.send(record);
		
		kafkaProducer.close();
		
		System.out.println("============== MESSAGE SENT ===========");
		
	}

}
