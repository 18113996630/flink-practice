package com.hrong.flink.model;

/**
 * @Description
 * @Author hrong
 * @Date 2019/5/9 11:52
 **/
public class StudentJava {
	private int id;
	private int classId;
	private String name;
	private int age;


	public StudentJava() {
	}

	public StudentJava(int id, int classId, String name, int age) {
		this.id = id;
		this.classId = classId;
		this.name = name;
		this.age = age;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getClassId() {
		return classId;
	}

	public void setClassId(int classId) {
		this.classId = classId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "StudentJava{" +
				"id=" + id +
				", classId=" + classId +
				", name='" + name + '\'' +
				", age=" + age +
				'}';
	}

	public static void main(String[] args) {
		String name = String.format("%d %d %s %d", 1, 2, "name", 12);
		System.out.println(name);
	}
}
