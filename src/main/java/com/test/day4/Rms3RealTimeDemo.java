package com.test.day4;

import org.apache.beam.runners.direct.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Rms3RealTimeDemo {
	public static interface Rms3RealTimeDemoOptions extends PipelineOptions{
		@Description("name")
		@Default.String("kafka save as files by window")
		String getName();
		void setName(String name);
		
		@Description("topic name")
		@Default.String("test")
		String getTopic();
		void setTopic(String topic);
		
		@Description("bootstrapServers,split by ,")
		@Default.String("test")
		String getBootstrapServers();
		void setBootstrapServers(String bootstrapServers);
		
		@Description("hbase.zookeeper.quorum")
		@Default.String("localhost")
		String getQuorum();
		void setQuorum(String quorum);
		
		@Description("hbase.zookeeper.property.clientPort")
		@Default.String("2181")
		String getHbaseClientPort();
		void setHbaseClientPort(String hbaseClientPort);
		
		@Description("zookeeper.znode.parent")
		@Default.String("test")
		String getHbaseParent();
		void setHbaseParent(String hbaseParent);
		
		@Description("hbase table")
		@Default.String("rm2_table")
		String getHbaseTable();
		void setHbaseTable(String hbaseTable);
	}
	public static void main(String[] args) {
		Rms3RealTimeDemoOptions options = PipelineOptionsFactory
				.fromArgs(args)
				.withValidation()
				.as(Rms3RealTimeDemoOptions.class);
		
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", options.getQuorum());
		configuration.set("hbase.zookeeper.property.clientPort", options.getHbaseClientPort());
		//configuration.set("zookeeper.znode.parent", options.getHbaseParent());
		
		Pipeline p = Pipeline.create(options);
		
		PCollection<KV<String, String>> pc1 = 
				p.apply(HBaseIO.read()
					.withConfiguration(configuration)
					.withTableId(options.getHbaseTable()))
				.apply(ParDo.<Result,KV<String,String>>of(new DoFn<Result, KV<String,String>>(){
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Result result = c.element();
						KV<String,String> line = null;
						String deviceId = "";
						String timestamp = "";
						for(Cell cell : result.listCells()) {
							if(new String(CellUtil.cloneQualifier(cell)).equals("timestamp")) {
								timestamp = new String((CellUtil.cloneValue(cell)));
							}
							if(new String(CellUtil.cloneQualifier(cell)).equals("deviceId")) {
								deviceId=new String((CellUtil.cloneValue(cell)));
							}
						}
						line = KV.of(deviceId, timestamp);
						System.out.println(line);
						c.output(line);
					}
				}));
		
		 /*p.apply(KafkaIO.<Long,String>read()
				.withBootstrapServers(options.getBootstrapServers())
				.withTopic(options.getTopic())
				.withKeyDeserializer(LongDeserializer.class)
				.withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.<String,Object>of(
						"auto.offset.reset", "latest",
						"group.id", "my_group_id4",
						"enable.auto.commit",true
						))
				.withoutMetadata())
		 .apply(Values.<String>create())
		 .apply(ParDo.<String, KV<String,String>>of(new DoFn<String, KV<String,String>>(){
			private static final long serialVersionUID = 2L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String line = c.element();
			}
		}));*/
		
		p.run().waitUntilFinish();
		
	}
}
