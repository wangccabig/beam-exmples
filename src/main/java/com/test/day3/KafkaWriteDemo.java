package com.test.day3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaWriteDemo {
	public static interface KafkaWindowDemoOptions extends PipelineOptions{
		@Description("the flow name")
		@Default.String("default name")
		String getName();
		void setName(String value);
		
		@Description("kafka topic name")
		@Default.String("test-kafka-write")
		String getTopic();
		void setTopic(String value);
		
		@Description("kafka bootstrap servers,split by ,")
		@Default.String("localhost:9092")
		String getBootstrapServers();
		void setBootstrapServers(String bootstrapServers);
	}
	
	public static void main(String[] args) {
		PipelineOptionsFactory.register(KafkaWindowDemoOptions.class);
		KafkaWindowDemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaWindowDemoOptions.class);
		Pipeline p = Pipeline.create(options);
		p.apply(options.getName(),TextIO.read().from("src/main/resources/test1.txt"))
		.apply(MapElements.via(new SimpleFunction<String, KV<Long,String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public KV<Long, String> apply(String input) {
				// TODO Auto-generated method stub
				return KV.of(new Long(1), input);
			}
		}))
		.apply(KafkaIO.<Long,String>write().withTopic(options.getTopic())
				.withBootstrapServers(options.getBootstrapServers())
				.withKeySerializer(LongSerializer.class)
				.withValueSerializer(StringSerializer.class));
		
		p.run().waitUntilFinish();
	}
}
