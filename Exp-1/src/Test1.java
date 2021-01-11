package Test;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.Math;

public class Test1 {
	// 存储有关数据库的静态变量
	public static class DataBase {
		public static Connection con; // 声明Connection对象
		public static Statement sql; // 声明Statement对象
		public static ResultSet res; // 声明ResultSet对象
		public static final String user = "root";// 数据库用户名
		public static final String password = "12345";// 数据库密码
	}

	public static class Array {
		public static HashMap<Integer, Double> map = new HashMap<Integer, Double>();// 记录第一问北京学生每个科目的成绩
		public static ArrayList<String> cList = new ArrayList<>();// 用于记录第四问的课程成绩
		public static ArrayList<String> ConstitutionList = new ArrayList<>();// 用于记录第四问Constitution成绩
		public static ArrayList<Double> cc = new ArrayList<>();// 记录课程相关性的值
		public static ArrayList<Double> css = new ArrayList<>();// 记录体能成绩相关性的值
		public static ArrayList<String> gzList = new ArrayList<>();// 记录广州女同学的体能测试成绩
		public static ArrayList<String> shList = new ArrayList<>();// 记录上海女同学的体能测试成绩
		public static ArrayList<String> IDList = new ArrayList<>();// 用于记录已经存在的ID号
	}

	public static class Question {
		public static int flu = 0;// 记录第一问有多少北京学生
		public static int maleNum = 0;// 统计来自广州的且成绩1大于80分且成绩9大于9.0的学生人数
		public static double correlations = 0.0;// 相关性
	}

	public static void main(String[] args) throws IOException {
		FileWriter fw = new FileWriter("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\merged.txt");// merged.txt
		List<String> list = new ArrayList<>();// 存储
		readTxt(list, fw);// 读取txt文件数据
		readMysql(list, fw);// 读取数据库数据
		sorted(list);// 按ID号从小到大进行排序
		fw.close();// 关闭流
		sortWrite(list);// 将排序好的数据内容写入到sorted.txt文件中,同时调用function1-3
		// 将广州学生的体能成绩进行量化
		double[] gzquantization = quantization(Test1.Array.gzList);
		// 求广州学生体能测试成绩均值
		double gzx = Mean(Test1.Array.gzList, gzquantization);
		// 对上海学生的体能测试成绩量化
		double[] shquantization = quantization(Test1.Array.shList);
		// 求上海学生体能测试成绩均值
		double shx = Mean(Test1.Array.shList, shquantization);

		// 输出问题一：北京学生平均成绩
		Double[] funcAvg = funcAvg();
		System.out.println("学生中家乡在北京的所有课程平均成绩如下：");
		for (int i = 0; i < funcAvg.length; i++) {
			System.out.print("Question" + (i + 1) + ":" + funcAvg[i] + "|");
		}
		// 输出问题二：学生中家乡在广州，课程1在80分以上，且课程10在9分以上的男同学的数量
		System.out.println("\r\n\r\n学生中家乡在广州，课程1在80分以上，且课程9在9分以上的男同学的数量：" + Test1.Question.maleNum);
		// 输出问题三：比较广州和上海的女生体测成绩
		if (gzx > shx) {
			System.out.println("\r\n广州的学生体能比较强");
		} else if (gzx == shx) {
			System.out.println("\r\n两地学生的体能一样强");
		} else if (gzx < shx) {
			System.out.println("\r\n上海的学生体能比较强");
		}
		System.out.print("广州学生体能成绩平均值：" + gzx + "\r\n");
		System.out.print("上海学生体能成绩平均值：" + shx + "\r\n\r\n");
		correlation();// 计算所有课程与体测成绩之间的相关性
	}

	// 数据库连接方法
	public Connection getConnection() { // 建立返回值为Connection的方法
		try { // 加载数据库驱动
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("数据库驱动加载成功");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try { // 通过访问数据库的URL获取数据库连接对象
			Test1.DataBase.con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", Test1.DataBase.user,
					Test1.DataBase.password);
			System.out.println("数据库连接成功");
			System.out.print('\n');
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Test1.DataBase.con;// 按方法要求返回一个Connection对象
	}

	// 读取数据库学生数据的方法
	public static void readMysql(List<String> list, FileWriter fw) {
		Test1 c = new Test1(); // 创建本类对象
		Test1.DataBase.con = c.getConnection(); // 与数据库建立连接
		try {
			Test1.DataBase.sql = Test1.DataBase.con.createStatement(); // 实例化Statement对象
			Test1.DataBase.res = Test1.DataBase.sql.executeQuery("select * from student");// 定义sql查询语句对象，查询数据库里的student表(本地已经导入csv)
			while (Test1.DataBase.res.next()) { // 如果当前语句不是最后一条，则进入循环
				String id = Test1.DataBase.res.getString("ID"); // 获取id字段值
				String name = Test1.DataBase.res.getString("Name"); // 获取Name字段值
				String city = Test1.DataBase.res.getString("City"); // 获取City字段值
				String gender = Test1.DataBase.res.getString("Gender"); // 获取Gender字段值
				String height = Test1.DataBase.res.getString("Height"); // 获取Height字段值
				String c1 = Test1.DataBase.res.getString("C1");// 获取c1字段值
				String c2 = Test1.DataBase.res.getString("C2");// 获取c2字段值
				String c3 = Test1.DataBase.res.getString("C3");// 获取c3字段值
				String c4 = Test1.DataBase.res.getString("C4");// 获取c4字段值
				String c5 = Test1.DataBase.res.getString("C5");// 获取c5字段值
				String c6 = Test1.DataBase.res.getString("C6");// 获取c6字段值
				String c7 = Test1.DataBase.res.getString("C7");// 获取c7字段值
				String c8 = Test1.DataBase.res.getString("C8");// 获取c8字段值
				String c9 = Test1.DataBase.res.getString("C9");// 获取c9字段值
				String c10 = Test1.DataBase.res.getString("C10");// 获取c10字段值
				String constitution = Test1.DataBase.res.getString("Constitution");// 获取Constitution字段值
				String sf;
				// id1-3用于校正ID号统一格式
				String id1 = "20200" + id;
				String id2 = "2020" + id;
				String id3 = "202" + id;
				float heights = Float.parseFloat(height) / 100;// 将身高化为浮点数类型
				if (constitution.isEmpty()) {
					constitution = "NULL";// 如果txt文件中的constitution值是空的话，赋值为NULL
				}
				String idName1 = "," + name + "," + city + "," + "male" + "," + heights + "," + c1 + "," + c2 + "," + c3
						+ "," + c4 + "," + c5 + "," + c6 + "," + c7 + "," + c8 + "," + c9 + "," + c10 + ","
						+ constitution + "\r\n";
				String idName2 = "," + name + "," + city + "," + "female" + "," + heights + "," + c1 + "," + c2 + ","
						+ c3 + "," + c4 + "," + c5 + "," + c6 + "," + c7 + "," + c8 + "," + c9 + "," + c10 + ","
						+ constitution + "\r\n";
				// 把boy或者girl替换为male和female
				if (Integer.parseInt(id) < 10) {// id号在1-9
					if (gender.equals("boy")) {
						sf = id1 + idName1;
					} else {
						sf = id1 + idName2;
					}
				} else if (10 <= Integer.parseInt(id) && Integer.parseInt(id) < 100) {// id号在10-99
					if (gender.equals("boy")) {
						sf = id2 + idName1;
					} else {
						sf = id2 + idName2;
					}
				} else {// id号在100以上
					if (gender.equals("boy")) {
						sf = id3 + idName1;
					} else {
						sf = id3 + idName2;
					}
				}
				// 当不包含已经存在的ID号时才添加
				if (!Test1.Array.IDList.contains(sf.split(",")[0])) {
					fw.write(sf);
					list.add(sf);
				}
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	// 读写txt文件的方法
	public static void readTxt(List<String> list, FileWriter fw) throws IOException {
		// 读取txt文件
		BufferedReader br = new BufferedReader(
				new FileReader("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\student.txt"));// merged.txt
		String line;
		while ((line = br.readLine()) != null) {
			StringBuilder stringBuilder = new StringBuilder();// 定义可变字符串流对象，一个流对象就是一条学生信息
			for (int i = 0; i < line.length(); i++) {
				if (i < line.length() - 1 && (line.charAt(i) == ',' && line.charAt(i + 1) == ',')) {
					char a = line.charAt(i);
					stringBuilder.append(a + "0");// 如果此元素和下一个元素都为逗号，则往两个逗号之间填充字符0
				} else {
					char a = line.charAt(i);
					stringBuilder.append(a + "");// 否则不填充
				}
			}
			String[] split1 = stringBuilder.toString().split(",");// 以逗号划分可变字符串
			if (stringBuilder.toString().endsWith(",")) {
				stringBuilder.append("NULL");// 如果该条学生信息以逗号结尾，则在结尾即学生体能成绩处填充字符NULL
			}
			if (split1[0].startsWith("202") && !Test1.Array.IDList.contains(split1[0])) {
				// 当学号以202开头且不是重复的学号时才将此行数据写入到文件中
				fw.write(stringBuilder + "\r\n");
				list.add((stringBuilder + "\r\n").toString());// 添加学生成绩信息
			}
			Test1.Array.IDList.add(split1[0]);// 存放已经添加的学号
		}
		br.close();// 关闭流
	}

	// 将排序好的内容写入到sorted.txt文件中的方法
	public static void sortWrite(List<String> list) throws IOException {
		// 定义缓冲流对象
		BufferedReader brr = new BufferedReader(
				new FileReader("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\merged.txt"));// merged.txt
		BufferedWriter brw = new BufferedWriter(
				new FileWriter("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\sorted.txt"));// sorted.txt
		String line2;
		while ((line2 = brr.readLine()) != null) {
			function1(line2);// 统计来自北京的学生每科的平均成绩
			function2(line2);// 统计学生中家乡在广州，课程1在80分以上，且课程10在9分以上的男同学的数量
			function3(line2);// 比较广州和上海两地女生的平均体能测试成绩
		}
		for (String s : list) {
			brw.write(s);// 将排序好的内容写入到sorted.txt文件里
		}
		brr.close();
		brw.close();
	}

	// 将最终数据结果排序的方法
	public static void sorted(List<String> list) {
		Comparator<String> comparator = new Comparator<String>() {// 重写comparator接口
			@Override
			public int compare(String s, String s1) {// 从小到大的顺序
				return Integer.parseInt(s.split(",")[0]) - Integer.parseInt(s1.split(",")[0]);
			}
		};
		Collections.sort(list, comparator);// 按照ID号大小进行排序
	}

	// 计算第一问每个科目的平均值的方法
	public static Double[] funcAvg() {
		Double[] avg = new Double[10];// 记录各科的平均值
		for (int i = 0; i < 10; i++) {
			avg[i] = Test1.Array.map.get(i) / Test1.Question.flu;
		}
		return avg;
	}

	// 获取学生中家乡在Beijing的所有课程的成绩的方法
	public static void function1(String line2) {
		String[] split = line2.split(",");
		if (split[2].equals("Beijing")) {
			for (int i = 0; i < 10; i++) {
				if (Test1.Question.flu == 0) {
					Test1.Array.map.put(i, Double.parseDouble(split[i + 5]));
				} else {
					Test1.Array.map.put(i, Test1.Array.map.get(i) + Double.parseDouble(split[i + 5]));
				}
			}
			Test1.Question.flu++;
		}
	}

	// 统计学生中家乡在广州，课程1在80分以上，且课程10在9分以上的男同学的数量的方法
	public static void function2(String line2) {
		String[] split = line2.split(",");
		if (split[2].equals("Guangzhou") && Float.parseFloat(split[5]) >= 80.0 && Float.parseFloat(split[13]) >= 9.0
				&& split[3].equals("male")) {
			Test1.Question.maleNum++;// 统计来自广州的且成绩1大于80分且成绩9大于9.0的学生人数
		}
	}

	// 获取广州和上海两地的女生体能测试成绩的方法
	public static void function3(String line2) {
		String[] split = line2.split(",");
		if (split[2].equals("Guangzhou") && split[3].equals("female")) {
			Test1.Array.gzList.add(split[15]);
		}
		if (split[2].equals("Shanghai") && split[3].equals("female")) {
			Test1.Array.shList.add(split[15]);
		}
	}

	// 计算相关性的方法
	public static void correlation() throws IOException {
		for (int i = 1; i <= 9; i++) {
			BufferedReader brr = new BufferedReader(
					new FileReader("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\sorted.txt"));// sorted.txt
			String line2;
			while ((line2 = brr.readLine()) != null) {
				String[] split = line2.split(",");
				Test1.Array.cList.add(split[(i + 4)]);
				Test1.Array.ConstitutionList.add(split[15]);
			}
			// 计算相关性
			double course = MeanCourse(Test1.Array.cList);// 得到课程均值
			double varianceCourse = varianceCourse(Test1.Array.cList, course);// 得到课程方差
			for (String cl : Test1.Array.cList) {
				Test1.Array.cc.add((Double.parseDouble(cl) - course) / Math.sqrt(varianceCourse));// 带入公式
			}
			double[] cssQuantization = quantization(Test1.Array.ConstitutionList);// 得到量化后的数组
			double csCourse = Mean(Test1.Array.ConstitutionList, cssQuantization);// 得到体测成绩均值
			double varianceCsCourse = variance(Test1.Array.ConstitutionList, cssQuantization, csCourse);// 计算方差
			for (String csl : Test1.Array.ConstitutionList) {
				double ds = 0.0;
				if (csl.equals("general")) {
					ds = 80;
				} else if (csl.equals("good")) {
					ds = 90;
				} else if (csl.equals("excellent")) {
					ds = 100;
				} else if (csl.equals("bad")) {
					ds = 70;
				} else if (csl.equals("NULL")) {
					ds = 60;
				}
				Test1.Array.css.add((ds - csCourse) / Math.sqrt(varianceCsCourse));
			}
			for (int j = 0; j < Test1.Array.cc.size(); j++) {
				Test1.Question.correlations += Test1.Array.cc.get(j) * Test1.Array.css.get(j);// 累加计算相关性
			}
			System.out.println("课程" + i + "与体测成绩的相关性为:" + Test1.Question.correlations);
			Test1.Question.correlations = 0;// 变量置0
			Test1.Array.cList.clear();// 清除列表给下一课程用
			Test1.Array.ConstitutionList.clear();// 清除列表给下一课程用
			Test1.Array.cc.clear();// 清除列表给下一课程用
			Test1.Array.css.clear();// 关闭流
			brr.close();// 关闭流
		}
	}

	// 量化的方法
	public static double[] quantization(ArrayList<String> list) {
		double[] mq = new double[1024];
		for (String s : list) {
			// 对文本数据进行量化
			if (s.equals("general")) {
				mq[0] += 80.0;
			} else if (s.equals("good")) {
				mq[1] += 90.0;
			} else if (s.equals("excellent")) {
				mq[2] += 100.0;
			} else if (s.equals("bad")) {
				mq[3] += 70.0;
			} else if (s.equals("NULL")) {
				mq[4] += 60.0;
			}
		}
		return mq;// 返回存储这五个成绩指标类型的数组
	}

	// 计算体测成绩均值的方法
	public static double Mean(ArrayList<String> list, double[] mm) {
		double num = 0.0;
		for (Double d : mm) {
			num += d;
		}
		return (num) / list.size();// 求学生体能测试成绩均值
	}

	// 计算单个课程成绩均值的方法
	public static double MeanCourse(ArrayList<String> list) {
		double num = 0.0;
		for (String s : list) {
			num += Double.parseDouble(s);
		}
		return (num) / list.size();
	}

	// 计算体测成绩方差
	public static double variance(ArrayList<String> list, double[] mm, double x) {
		double ff = (mm[0] / 80 * (80 - x) * (80 - x) + (mm[1] / 90) * (90 - x) * (90 - x)
				+ (mm[2] / 100) * (100 - x) * (100 - x) + (mm[3] / 70) * (70 - x) * (70 - x)
				+ (mm[4] / 60) * (60 - x) * (60 - x)) / list.size();
		return ff;
	}

	// 计算课程成绩方差
	public static double varianceCourse(ArrayList<String> list, double x) {
		double ff = 0.0;
		for (String s : list) {
			double v = Double.parseDouble(s);
			ff += (v - MeanCourse(list)) * (v - MeanCourse(list)) / list.size();
		}
		return ff;
	}
}