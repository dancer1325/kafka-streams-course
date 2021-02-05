package com.github.simplesteph.udemy.kafka.streams;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class WordCountApp {
	public static void main(String[] args) {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// 1 - stream from Kafka. Versions < 10. was StreamBuilder()
		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> textLines = builder.stream("word-count-input");
		KTable<String, Long> wordCounts = textLines
				// 2 - map values to lowercase
				.mapValues(textLine -> textLine.toLowerCase())
				//Alternative 1. Reference method:
				// .mapValues(String::toLowerCase)
				//Alternative 2, using Java 7:
				// .mapValues(new ValueMapper<String, Object>() {
//                  @Override
//                  publilc Object apply(String value) {
//		                return null;
//                  }
//              })
				// 3 - flatmap values split by space
				.flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
//              To split by just 1 blank space
//                .flatMapValues(textLine -> Arrays.asList(textLine.split(" ")))
				// 4 - select key to apply a key (we discard the old key)
				.selectKey((key, word) -> word)
				// 5 - group by key before aggregation
				.groupByKey()
				// 6 - count occurrences, storing under the name Counts
				.count("Counts");

		// 7 - to in order to write the results back to kafka.
        // The Serdes to specify are related to the KTable to serialize/deserialize != Stream Application's properties
		wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

		KafkaStreams streams = new KafkaStreams(builder, config);
		//To start the KafkaStream app
		streams.start();


		// shutdown hook to correctly close the streams application
        //You could optionally provide a timeout to close it
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		// Update:
		// print the topology every 10 seconds for learning purposes
		while (true) {
			System.out.println(streams.toString());
			//You could also print it using the log4j library
			try {
				Thread.sleep(5000);
			}
			catch (InterruptedException e) {
				break;
			}
		}

	}
}
