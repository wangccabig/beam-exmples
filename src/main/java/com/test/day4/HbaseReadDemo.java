package com.test.day4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;

public class HbaseReadDemo {
	public static void main(String[] args) {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "localhost");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//configuration.set("hbase.master", "192.168.56.14:16000"); 
		
		PipelineOptions pipopt = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(pipopt);
		p.apply(HBaseIO.read().withConfiguration(configuration).withTableId("rm2_test"))
			.apply(ParDo.<Result, String>of(new DoFn<Result, String>(){
				@ProcessElement
				public void processElement(ProcessContext c) {
					Result result = c.element();
					for (Cell cell : result.listCells()) {
			            String key = new String(CellUtil.cloneQualifier(cell));
			            String value = new String((CellUtil.cloneValue(cell)));
			            System.out.println(key + " => " + value);
			            System.out.println(1);
			        }
					c.output("hello");
				}
			}));
		
		p.run().waitUntilFinish();
	}
}
