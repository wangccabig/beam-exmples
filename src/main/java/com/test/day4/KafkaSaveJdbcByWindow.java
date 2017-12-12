package com.test.day4;

import java.sql.PreparedStatement;


import org.apache.beam.runners.direct.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaSaveJdbcByWindow {
	public static interface KafkaSaveFileByWindowOptions extends PipelineOptions{
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
		
		@Description("driverName")
		String getDriverName();
		void setDriverName(String driverName);
		
		@Description("database url")
		String getUrl();
		void setUrl(String url);
		
		@Description("database username")
		String getUserName();
		void setUserName(String username);
		
		@Description("database password")
		String getPassword();
		void setPassword(String password);
		
		@Description("insert sql")
		@Default.String("insert into test_window values(?)")
		String getSql();
		void setSql(String sql);
		
	}
	
	public static void main(String[] args) {
		KafkaSaveFileByWindowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaSaveFileByWindowOptions.class);
		Pipeline p = Pipeline.create(options);
		
		p.apply(options.getName(),KafkaIO.<Long,String>read()
				.withTopic(options.getTopic())
				.withBootstrapServers(options.getBootstrapServers())
				.withKeyDeserializer(LongDeserializer.class)
				.withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.<String, Object>of(
						"auto.offset.reset", "latest",
						"group.id", "my_group_id4",
						"enable.auto.commit",true))
				.withoutMetadata()
				)
		.apply(Values.<String>create())
		.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
		.apply(JdbcIO.<String>write()
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(options.getDriverName(), options.getUrl())
						.withUsername(options.getUserName())
						.withPassword(options.getPassword())
						)
				.withStatement(options.getSql())
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
						// TODO Auto-generated method stub
						preparedStatement.setString(1,element);
					}
				})
				);
		p.run().waitUntilFinish();
	}
}
