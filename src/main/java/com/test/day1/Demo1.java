package com.test.day1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class Demo1 {
	public static void main(String[] args) {
		PipelineOptions pipopt = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(pipopt);
		p.apply(TextIO.read().from("src/main/resources/test1.txt"))
		.apply("Word count",ParDo.of(new DoFn<String, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				String line = c.element();
				for(String word : line.split(" ")) {
					System.out.println(word);
					c.output(word);
				}
			}
		})).apply(Count.<String>perElement())
		.apply("FormatResult",MapElements.via(new SimpleFunction<KV<String,Long>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
		    public String apply(KV<String, Long> input) {
		        return input.getKey() + ":" + input.getValue();
		    }
		})).apply(TextIO.write().to("src/main/resources/result.txt"));
		p.run().waitUntilFinish();
	}
}
