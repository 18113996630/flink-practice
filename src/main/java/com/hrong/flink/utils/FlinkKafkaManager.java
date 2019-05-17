package com.hrong.flink.utils;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import java.util.Properties;

/**
 * @author 1011486768
 * @Description
 * @Date 2019/5/14 11:32
 **/
public class FlinkKafkaManager<T> {
	private String topic;
	private String groupId;
	private Properties properties;

	public FlinkKafkaManager(String topic, String groupId, Properties properties) {
		this.topic = topic;
		this.groupId = groupId;
		this.properties = properties;
		this.properties.setProperty("group.id", this.groupId);
		//为使用默认kafka的用户配置基础配置
		this.setDefaultKafkaProperties();
	}

	private void setDefaultKafkaProperties() {
		//启用auto commit offset, 每5s commit一次
		this.properties.setProperty("enable.auto.commit", "true");
		this.properties.setProperty("auto.commit.interval.ms", "5000");
	}

	public FlinkKafkaConsumer09<T> build(Class<T> clazz) {
		if (checkProperties()) {
			return new FlinkKafkaConsumer09<T>(topic, new ConsumerDeserializationSchema(clazz), properties);
		} else {
			return null;
		}
	}

	private boolean checkProperties() {
		boolean isValued = true;

		if (!properties.containsKey("bootstrap.servers")) {
			isValued = false;
		} else {
			String brokers = properties.getProperty("bootstrap.servers");
			if (brokers == null || brokers.isEmpty()) {
				isValued = false;
			}
		}

		if (this.topic == null || this.topic.isEmpty()) {
			isValued = false;
		}

		if (!properties.containsKey("group.id")) {
			isValued = false;
		} else {
			String groupId = properties.getProperty("group.id");
			if (groupId == null || groupId.isEmpty()) {
				isValued = false;
			}
		}

		return isValued;
	}
}
