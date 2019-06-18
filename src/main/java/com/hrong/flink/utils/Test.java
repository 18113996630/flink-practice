package com.hrong.flink.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author hrong
 * @ClassName Test
 * @Description
 * @Date 2019/5/31 11:39
 **/
public class Test {
	/**
	 * 日期转换
	 */
	private static final FastDateFormat FORMAT = FastDateFormat.getInstance("yyyy-MM-dd");
	private static List<ClassInfo> classInfos = new ArrayList<>(4);
	static {
		classInfos.add(ClassInfo.A);
		classInfos.add(ClassInfo.B);
		classInfos.add(ClassInfo.C);
		classInfos.add(ClassInfo.D);
	}

	public static void main(String[] args) {
		// 排班时间区间
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(new Date());

		Calendar endDate = Calendar.getInstance();
		endDate.setTime(new Date());
		endDate.add(Calendar.DAY_OF_YEAR, 3);


		// 传入截止日期，获取排班信息
		Map<Date, Map<TypeInfo, ClassInfo>> data = compute(startDate, endDate);
		for (Map.Entry<Date, Map<TypeInfo, ClassInfo>> dateMapEntry : data.entrySet()) {
			Date dateInfo = dateMapEntry.getKey();
			System.out.println(FORMAT.format(dateInfo) + " 的排班信息");
			Map<TypeInfo, ClassInfo> typeClasses = dateMapEntry.getValue();
			for (Map.Entry<TypeInfo, ClassInfo> entry : typeClasses.entrySet()) {
				TypeInfo typeInfo = entry.getKey();
				ClassInfo classInfo = entry.getValue();
				System.out.println(typeInfo + " - " + classInfo);
			}
		}

	}

	private static Map<Date, Map<TypeInfo, ClassInfo>> compute(Calendar startDate, Calendar endDate) {
		System.out.println("排班截止日期：" + FORMAT.format(endDate));
		Map<Date, Map<TypeInfo, ClassInfo>> result = new LinkedHashMap<>(100);
		// 根据班组进行排序
		classInfos.sort(Comparator.comparingInt(ClassInfo::getIndex));
		TypeInfo[] typeInfos = TypeInfo.values();

		int flag = 0;
		while (startDate.before(endDate)) {
			// 通过递增的flag值对班组数量取余确定休息的班组，轮着休息
			int index = flag % classInfos.size();

			Map<TypeInfo, ClassInfo> typeInfoClassInfoMap = new LinkedHashMap<>(4);
			ClassInfo relax = classInfos.get(index);
			typeInfoClassInfoMap.put(TypeInfo.D, relax);
			// 将已排班的班组暂时删除
			classInfos.remove(index);
			for (int i = 0; i < typeInfos.length; i++) {
				// 对剩余的班组排班
				TypeInfo typeInfo = typeInfos[i];
				if (typeInfo == TypeInfo.D) {
					// 防止出现indexOutOfBoundsException
					break;
				}
				ClassInfo classInfo = classInfos.get(i);
				typeInfoClassInfoMap.put(typeInfo, classInfo);
			}
			result.put(startDate.getTime(), typeInfoClassInfoMap);
			classInfos.add(relax);
			classInfos.sort(Comparator.comparingInt(ClassInfo::getIndex));
			flag++;
			startDate.add(Calendar.DAY_OF_YEAR, 1);
		}
		return result;
	}
}

enum TypeInfo {
	/**
	 * 班次信息
	 */
	A(0, "白班"),
	B(1, "中班"),
	C(2, "晚班"),
	D(3, "休息"),
	;

	TypeInfo(int index, String name) {
		this.index = index;
		this.name = name;
	}

	private int index;
	private String name;

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}
}

enum ClassInfo {
	/**
	 * 班组信息
	 */
	A(0, "甲班"),
	B(1, "乙班"),
	C(2, "丙班"),
	D(3, "丁班"),
	;

	ClassInfo(int index, String name) {
		this.index = index;
		this.name = name;
	}

	private int index;
	private String name;

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}
}