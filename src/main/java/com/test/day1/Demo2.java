package com.test.day1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
public class Demo2 {
	public static void main(String[] args) {
		PipelineOptions pipopt = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(pipopt);
		
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "localhost");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//configuration.set("hbase.master", "192.168.56.14:16000"); 
		
		Scan scan = new Scan();
		scan.addColumn("cf".getBytes(), "deviceId".getBytes());
		//scan.setBatch(1000);
		p.apply(HBaseIO.read().withConfiguration(configuration).withTableId("rm2_test"))
		.apply("print result",ParDo.of(new DoFn<Result, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				Result r = c.element();
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily())+":" + keyValue.getKeyString() + "=:" + new String(keyValue.getValue())); 
                } 
			}
		}));
		p.run();
	}
}
