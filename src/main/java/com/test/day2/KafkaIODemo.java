package com.test.day2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.common.collect.ImmutableMap;

public class KafkaIODemo {
	public static void main(String[] args) {
		PipelineOptions pipopt = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(pipopt);
		p.apply("kafka demo",KafkaIO.<Long,String>read()
				.withBootstrapServers("localhost:9092")
				.withTopic("Kafka.IO.Test")
				.withKeyDeserializer(LongDeserializer.class)
				.withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.<String,Object>of(
						//"auto.offset.reset", "",
                        "group.id", "my_group_id4",
                        "offsets.storage","zookeeper",
                        //"dual.commit.enabled","true",
                        "enable.auto.commit",true
                        ))
				.withoutMetadata())
		.apply(Values.<String>create())
		.apply("print result", ParDo.of(new DoFn<String, String>(){
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		p.run().waitUntilFinish();
	}
}
