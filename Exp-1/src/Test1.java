package Test;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.Math;

public class Test1 {
	// �洢�й����ݿ�ľ�̬����
	public static class DataBase {
		public static Connection con; // ����Connection����
		public static Statement sql; // ����Statement����
		public static ResultSet res; // ����ResultSet����
		public static final String user = "root";// ���ݿ��û���
		public static final String password = "12345";// ���ݿ�����
	}

	public static class Array {
		public static HashMap<Integer, Double> map = new HashMap<Integer, Double>();// ��¼��һ�ʱ���ѧ��ÿ����Ŀ�ĳɼ�
		public static ArrayList<String> cList = new ArrayList<>();// ���ڼ�¼�����ʵĿγ̳ɼ�
		public static ArrayList<String> ConstitutionList = new ArrayList<>();// ���ڼ�¼������Constitution�ɼ�
		public static ArrayList<Double> cc = new ArrayList<>();// ��¼�γ�����Ե�ֵ
		public static ArrayList<Double> css = new ArrayList<>();// ��¼���ܳɼ�����Ե�ֵ
		public static ArrayList<String> gzList = new ArrayList<>();// ��¼����Ůͬѧ�����ܲ��Գɼ�
		public static ArrayList<String> shList = new ArrayList<>();// ��¼�Ϻ�Ůͬѧ�����ܲ��Գɼ�
		public static ArrayList<String> IDList = new ArrayList<>();// ���ڼ�¼�Ѿ����ڵ�ID��
	}

	public static class Question {
		public static int flu = 0;// ��¼��һ���ж��ٱ���ѧ��
		public static int maleNum = 0;// ͳ�����Թ��ݵ��ҳɼ�1����80���ҳɼ�9����9.0��ѧ������
		public static double correlations = 0.0;// �����
	}

	public static void main(String[] args) throws IOException {
		FileWriter fw = new FileWriter("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\merged.txt");// merged.txt
		List<String> list = new ArrayList<>();// �洢
		readTxt(list, fw);// ��ȡtxt�ļ�����
		readMysql(list, fw);// ��ȡ���ݿ�����
		sorted(list);// ��ID�Ŵ�С�����������
		fw.close();// �ر���
		sortWrite(list);// ������õ���������д�뵽sorted.txt�ļ���,ͬʱ����function1-3
		// ������ѧ�������ܳɼ���������
		double[] gzquantization = quantization(Test1.Array.gzList);
		// �����ѧ�����ܲ��Գɼ���ֵ
		double gzx = Mean(Test1.Array.gzList, gzquantization);
		// ���Ϻ�ѧ�������ܲ��Գɼ�����
		double[] shquantization = quantization(Test1.Array.shList);
		// ���Ϻ�ѧ�����ܲ��Գɼ���ֵ
		double shx = Mean(Test1.Array.shList, shquantization);

		// �������һ������ѧ��ƽ���ɼ�
		Double[] funcAvg = funcAvg();
		System.out.println("ѧ���м����ڱ��������пγ�ƽ���ɼ����£�");
		for (int i = 0; i < funcAvg.length; i++) {
			System.out.print("Question" + (i + 1) + ":" + funcAvg[i] + "|");
		}
		// ����������ѧ���м����ڹ��ݣ��γ�1��80�����ϣ��ҿγ�10��9�����ϵ���ͬѧ������
		System.out.println("\r\n\r\nѧ���м����ڹ��ݣ��γ�1��80�����ϣ��ҿγ�9��9�����ϵ���ͬѧ��������" + Test1.Question.maleNum);
		// ������������ȽϹ��ݺ��Ϻ���Ů�����ɼ�
		if (gzx > shx) {
			System.out.println("\r\n���ݵ�ѧ�����ܱȽ�ǿ");
		} else if (gzx == shx) {
			System.out.println("\r\n����ѧ��������һ��ǿ");
		} else if (gzx < shx) {
			System.out.println("\r\n�Ϻ���ѧ�����ܱȽ�ǿ");
		}
		System.out.print("����ѧ�����ܳɼ�ƽ��ֵ��" + gzx + "\r\n");
		System.out.print("�Ϻ�ѧ�����ܳɼ�ƽ��ֵ��" + shx + "\r\n\r\n");
		correlation();// �������пγ������ɼ�֮��������
	}

	// ���ݿ����ӷ���
	public Connection getConnection() { // ��������ֵΪConnection�ķ���
		try { // �������ݿ�����
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("���ݿ��������سɹ�");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try { // ͨ���������ݿ��URL��ȡ���ݿ����Ӷ���
			Test1.DataBase.con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", Test1.DataBase.user,
					Test1.DataBase.password);
			System.out.println("���ݿ����ӳɹ�");
			System.out.print('\n');
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Test1.DataBase.con;// ������Ҫ�󷵻�һ��Connection����
	}

	// ��ȡ���ݿ�ѧ�����ݵķ���
	public static void readMysql(List<String> list, FileWriter fw) {
		Test1 c = new Test1(); // �����������
		Test1.DataBase.con = c.getConnection(); // �����ݿ⽨������
		try {
			Test1.DataBase.sql = Test1.DataBase.con.createStatement(); // ʵ����Statement����
			Test1.DataBase.res = Test1.DataBase.sql.executeQuery("select * from student");// ����sql��ѯ�����󣬲�ѯ���ݿ����student��(�����Ѿ�����csv)
			while (Test1.DataBase.res.next()) { // �����ǰ��䲻�����һ���������ѭ��
				String id = Test1.DataBase.res.getString("ID"); // ��ȡid�ֶ�ֵ
				String name = Test1.DataBase.res.getString("Name"); // ��ȡName�ֶ�ֵ
				String city = Test1.DataBase.res.getString("City"); // ��ȡCity�ֶ�ֵ
				String gender = Test1.DataBase.res.getString("Gender"); // ��ȡGender�ֶ�ֵ
				String height = Test1.DataBase.res.getString("Height"); // ��ȡHeight�ֶ�ֵ
				String c1 = Test1.DataBase.res.getString("C1");// ��ȡc1�ֶ�ֵ
				String c2 = Test1.DataBase.res.getString("C2");// ��ȡc2�ֶ�ֵ
				String c3 = Test1.DataBase.res.getString("C3");// ��ȡc3�ֶ�ֵ
				String c4 = Test1.DataBase.res.getString("C4");// ��ȡc4�ֶ�ֵ
				String c5 = Test1.DataBase.res.getString("C5");// ��ȡc5�ֶ�ֵ
				String c6 = Test1.DataBase.res.getString("C6");// ��ȡc6�ֶ�ֵ
				String c7 = Test1.DataBase.res.getString("C7");// ��ȡc7�ֶ�ֵ
				String c8 = Test1.DataBase.res.getString("C8");// ��ȡc8�ֶ�ֵ
				String c9 = Test1.DataBase.res.getString("C9");// ��ȡc9�ֶ�ֵ
				String c10 = Test1.DataBase.res.getString("C10");// ��ȡc10�ֶ�ֵ
				String constitution = Test1.DataBase.res.getString("Constitution");// ��ȡConstitution�ֶ�ֵ
				String sf;
				// id1-3����У��ID��ͳһ��ʽ
				String id1 = "20200" + id;
				String id2 = "2020" + id;
				String id3 = "202" + id;
				float heights = Float.parseFloat(height) / 100;// ����߻�Ϊ����������
				if (constitution.isEmpty()) {
					constitution = "NULL";// ���txt�ļ��е�constitutionֵ�ǿյĻ�����ֵΪNULL
				}
				String idName1 = "," + name + "," + city + "," + "male" + "," + heights + "," + c1 + "," + c2 + "," + c3
						+ "," + c4 + "," + c5 + "," + c6 + "," + c7 + "," + c8 + "," + c9 + "," + c10 + ","
						+ constitution + "\r\n";
				String idName2 = "," + name + "," + city + "," + "female" + "," + heights + "," + c1 + "," + c2 + ","
						+ c3 + "," + c4 + "," + c5 + "," + c6 + "," + c7 + "," + c8 + "," + c9 + "," + c10 + ","
						+ constitution + "\r\n";
				// ��boy����girl�滻Ϊmale��female
				if (Integer.parseInt(id) < 10) {// id����1-9
					if (gender.equals("boy")) {
						sf = id1 + idName1;
					} else {
						sf = id1 + idName2;
					}
				} else if (10 <= Integer.parseInt(id) && Integer.parseInt(id) < 100) {// id����10-99
					if (gender.equals("boy")) {
						sf = id2 + idName1;
					} else {
						sf = id2 + idName2;
					}
				} else {// id����100����
					if (gender.equals("boy")) {
						sf = id3 + idName1;
					} else {
						sf = id3 + idName2;
					}
				}
				// ���������Ѿ����ڵ�ID��ʱ�����
				if (!Test1.Array.IDList.contains(sf.split(",")[0])) {
					fw.write(sf);
					list.add(sf);
				}
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	// ��дtxt�ļ��ķ���
	public static void readTxt(List<String> list, FileWriter fw) throws IOException {
		// ��ȡtxt�ļ�
		BufferedReader br = new BufferedReader(
				new FileReader("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\student.txt"));// merged.txt
		String line;
		while ((line = br.readLine()) != null) {
			StringBuilder stringBuilder = new StringBuilder();// ����ɱ��ַ���������һ�����������һ��ѧ����Ϣ
			for (int i = 0; i < line.length(); i++) {
				if (i < line.length() - 1 && (line.charAt(i) == ',' && line.charAt(i + 1) == ',')) {
					char a = line.charAt(i);
					stringBuilder.append(a + "0");// �����Ԫ�غ���һ��Ԫ�ض�Ϊ���ţ�������������֮������ַ�0
				} else {
					char a = line.charAt(i);
					stringBuilder.append(a + "");// �������
				}
			}
			String[] split1 = stringBuilder.toString().split(",");// �Զ��Ż��ֿɱ��ַ���
			if (stringBuilder.toString().endsWith(",")) {
				stringBuilder.append("NULL");// �������ѧ����Ϣ�Զ��Ž�β�����ڽ�β��ѧ�����ܳɼ�������ַ�NULL
			}
			if (split1[0].startsWith("202") && !Test1.Array.IDList.contains(split1[0])) {
				// ��ѧ����202��ͷ�Ҳ����ظ���ѧ��ʱ�Ž���������д�뵽�ļ���
				fw.write(stringBuilder + "\r\n");
				list.add((stringBuilder + "\r\n").toString());// ���ѧ���ɼ���Ϣ
			}
			Test1.Array.IDList.add(split1[0]);// ����Ѿ���ӵ�ѧ��
		}
		br.close();// �ر���
	}

	// ������õ�����д�뵽sorted.txt�ļ��еķ���
	public static void sortWrite(List<String> list) throws IOException {
		// ���建��������
		BufferedReader brr = new BufferedReader(
				new FileReader("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\merged.txt"));// merged.txt
		BufferedWriter brw = new BufferedWriter(
				new FileWriter("Data_mining_Test\\src\\Data_mining_experiment1\\result_txt\\sorted.txt"));// sorted.txt
		String line2;
		while ((line2 = brr.readLine()) != null) {
			function1(line2);// ͳ�����Ա�����ѧ��ÿ�Ƶ�ƽ���ɼ�
			function2(line2);// ͳ��ѧ���м����ڹ��ݣ��γ�1��80�����ϣ��ҿγ�10��9�����ϵ���ͬѧ������
			function3(line2);// �ȽϹ��ݺ��Ϻ�����Ů����ƽ�����ܲ��Գɼ�
		}
		for (String s : list) {
			brw.write(s);// ������õ�����д�뵽sorted.txt�ļ���
		}
		brr.close();
		brw.close();
	}

	// ���������ݽ������ķ���
	public static void sorted(List<String> list) {
		Comparator<String> comparator = new Comparator<String>() {// ��дcomparator�ӿ�
			@Override
			public int compare(String s, String s1) {// ��С�����˳��
				return Integer.parseInt(s.split(",")[0]) - Integer.parseInt(s1.split(",")[0]);
			}
		};
		Collections.sort(list, comparator);// ����ID�Ŵ�С��������
	}

	// �����һ��ÿ����Ŀ��ƽ��ֵ�ķ���
	public static Double[] funcAvg() {
		Double[] avg = new Double[10];// ��¼���Ƶ�ƽ��ֵ
		for (int i = 0; i < 10; i++) {
			avg[i] = Test1.Array.map.get(i) / Test1.Question.flu;
		}
		return avg;
	}

	// ��ȡѧ���м�����Beijing�����пγ̵ĳɼ��ķ���
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

	// ͳ��ѧ���м����ڹ��ݣ��γ�1��80�����ϣ��ҿγ�10��9�����ϵ���ͬѧ�������ķ���
	public static void function2(String line2) {
		String[] split = line2.split(",");
		if (split[2].equals("Guangzhou") && Float.parseFloat(split[5]) >= 80.0 && Float.parseFloat(split[13]) >= 9.0
				&& split[3].equals("male")) {
			Test1.Question.maleNum++;// ͳ�����Թ��ݵ��ҳɼ�1����80���ҳɼ�9����9.0��ѧ������
		}
	}

	// ��ȡ���ݺ��Ϻ����ص�Ů�����ܲ��Գɼ��ķ���
	public static void function3(String line2) {
		String[] split = line2.split(",");
		if (split[2].equals("Guangzhou") && split[3].equals("female")) {
			Test1.Array.gzList.add(split[15]);
		}
		if (split[2].equals("Shanghai") && split[3].equals("female")) {
			Test1.Array.shList.add(split[15]);
		}
	}

	// ��������Եķ���
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
			// ���������
			double course = MeanCourse(Test1.Array.cList);// �õ��γ̾�ֵ
			double varianceCourse = varianceCourse(Test1.Array.cList, course);// �õ��γ̷���
			for (String cl : Test1.Array.cList) {
				Test1.Array.cc.add((Double.parseDouble(cl) - course) / Math.sqrt(varianceCourse));// ���빫ʽ
			}
			double[] cssQuantization = quantization(Test1.Array.ConstitutionList);// �õ������������
			double csCourse = Mean(Test1.Array.ConstitutionList, cssQuantization);// �õ����ɼ���ֵ
			double varianceCsCourse = variance(Test1.Array.ConstitutionList, cssQuantization, csCourse);// ���㷽��
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
				Test1.Question.correlations += Test1.Array.cc.get(j) * Test1.Array.css.get(j);// �ۼӼ��������
			}
			System.out.println("�γ�" + i + "�����ɼ��������Ϊ:" + Test1.Question.correlations);
			Test1.Question.correlations = 0;// ������0
			Test1.Array.cList.clear();// ����б����һ�γ���
			Test1.Array.ConstitutionList.clear();// ����б����һ�γ���
			Test1.Array.cc.clear();// ����б����һ�γ���
			Test1.Array.css.clear();// �ر���
			brr.close();// �ر���
		}
	}

	// �����ķ���
	public static double[] quantization(ArrayList<String> list) {
		double[] mq = new double[1024];
		for (String s : list) {
			// ���ı����ݽ�������
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
		return mq;// ���ش洢������ɼ�ָ�����͵�����
	}

	// �������ɼ���ֵ�ķ���
	public static double Mean(ArrayList<String> list, double[] mm) {
		double num = 0.0;
		for (Double d : mm) {
			num += d;
		}
		return (num) / list.size();// ��ѧ�����ܲ��Գɼ���ֵ
	}

	// ���㵥���γ̳ɼ���ֵ�ķ���
	public static double MeanCourse(ArrayList<String> list) {
		double num = 0.0;
		for (String s : list) {
			num += Double.parseDouble(s);
		}
		return (num) / list.size();
	}

	// �������ɼ�����
	public static double variance(ArrayList<String> list, double[] mm, double x) {
		double ff = (mm[0] / 80 * (80 - x) * (80 - x) + (mm[1] / 90) * (90 - x) * (90 - x)
				+ (mm[2] / 100) * (100 - x) * (100 - x) + (mm[3] / 70) * (70 - x) * (70 - x)
				+ (mm[4] / 60) * (60 - x) * (60 - x)) / list.size();
		return ff;
	}

	// ����γ̳ɼ�����
	public static double varianceCourse(ArrayList<String> list, double x) {
		double ff = 0.0;
		for (String s : list) {
			double v = Double.parseDouble(s);
			ff += (v - MeanCourse(list)) * (v - MeanCourse(list)) / list.size();
		}
		return ff;
	}
}