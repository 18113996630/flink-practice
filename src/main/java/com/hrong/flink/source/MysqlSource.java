package com.hrong.flink.source;

import com.hrong.flink.model.StudentJava;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Description
 * @Author huangrong
 * @Date 2019/5/8 23:19
 **/
public class MysqlSource extends RichSourceFunction<StudentJava> {
	private Connection connection;
	private PreparedStatement preparedStatement;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		Logger.getLogger("").setLevel(Level.OFF);
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://localhost:3306/test";
		connection = DriverManager.getConnection(url, "root", "123456");
		preparedStatement = connection.prepareStatement("select id, class_id, name, age from stu");
	}

	@Override
	public void close() throws Exception {
		super.close();
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run(SourceContext<StudentJava> sourceContext) throws Exception {
		ResultSet resultSet = preparedStatement.executeQuery();
		while (resultSet.next()) {
			int id = resultSet.getInt("id");
			int classId = resultSet.getInt("class_id");
			String name = resultSet.getString("name");
			int age = resultSet.getInt("age");
			StudentJava studentJava = new StudentJava(id, classId, name, age);
			sourceContext.collect(studentJava);
		}
	}

	@Override
	public void cancel() {

	}
}


