package com.wejo.seasonality;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;



public class DBCommon<T> {
	private String dbFileName;
	private String tableName;
	private Connection connection;
	private ArrayList<T> dataList;

//	public DBCommon(String dbFileName, String tableName) {
//		this.dbFileName = dbFileName;
//		this.tableName = tableName;
//	}
	public DBCommon(String tableName) {
	this.tableName = tableName;
}
	public DBCommon() {
	}
	
	private void open() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			this.connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.110.112:1521:orcl",
								"kopo","kopo");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void close() {
		if (this.connection != null) {
			try {
				this.connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		this.connection = null;
	}

	public void createTable(T t) {
		try {
			Class<?> dataClass = t.getClass();
			// Class.forName("com.politech.student.Student")

			Field[] dataClassFields = dataClass.getDeclaredFields();
			// student.getClass().getSimpleName()
			String query = "";
			for (Field field : dataClassFields) {
				if (!query.isEmpty()) {
					query = query + ",";
				}
				String fieldType = field.getType().toString();
				String fieldName = field.getName();
				if (fieldName.matches("idx") && fieldType.matches("int")) {
					query = query + fieldName + " INTEGER PRIMARY KEY AUTOINCREMENT";
				} else if (fieldType.matches("int")) {
					query = query + fieldName + " INTEGER";
				} else if (fieldType.matches("(double|float)")) {
					query = query + fieldName + " REAL";
				} else if (fieldType.matches(".*String")) {
					query = query + fieldName + " TEXT";
				}
			}
			if (this.connection == null) {
				this.open();
			}
			query = "CREATE TABLE " + this.tableName + "(" + query + ");";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public void insertData(T t) {
		try {
			Class<?> dataClass = t.getClass();
			// Class.forName("com.politech.student.Student")

			Field[] dataClassFields = dataClass.getDeclaredFields();
			// student.getClass().getSimpleName()
			String fieldString = "";
			String valueString = "";
			for (Field field : dataClassFields) {
				if (!fieldString.isEmpty()) {
					fieldString = fieldString + ",";
				}
				String fieldType = field.getType().toString();
				String fieldName = field.getName();
				if (fieldName.matches("idx")) {
					continue;
				}
				fieldString = fieldString + fieldName;
				if (!valueString.isEmpty()) {
					valueString = valueString + ",";
				}
				if (fieldType.matches(".*String")) {
					valueString = valueString + "'" + field.get(t) + "'";
				} else {
					valueString = valueString + field.get(t);
				}
			}
			if (this.connection == null) {
				this.open();
			}
			String query = "INSERT INTO " + this.tableName + "(" + fieldString + ") VALUES(" + valueString + ");";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public void updateData(T t, int whereIdx) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			String queryString = "";
			for (Field field : dataClassFields) {
				if (!queryString.isEmpty()) {
					queryString = queryString + ",";
				}
				String fieldType = field.getType().toString();
				String fieldName = field.getName();
				if (fieldName.matches("idx")) {
					continue;
				}
				if (fieldType.matches(".*String")) {
					queryString = queryString + fieldName + "=" + "'" + field.get(t) + "'";
				} else {
					queryString = queryString + fieldName + "=" + field.get(t);
				}
			}
			if (this.connection == null) {
				this.open();
			}
			String query = "UPDATE " + this.tableName + " SET " + queryString + " WHERE idx=" + whereIdx + ";";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public void deleteData(T t, int whereIdx) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			// int whereIdx = -1;
			for (Field field : dataClassFields) {
				String fieldType = field.getType().toString();
				String fieldName = field.getName();
				if (fieldName.matches("idx")) {
					// whereIdx = (Integer) field.get(t);
					continue;
				}
			}
			if (this.connection == null) {
				this.open();
			}
			String query = "DELETE FROM " + this.tableName + " WHERE idx=" + whereIdx + ";";
			Statement statement = this.connection.createStatement();
			int result = statement.executeUpdate(query);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public void selectData(T t) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
//			System.out.println("쿼리전왓니");
			String query = "SELECT * FROM "+ this.tableName;
//			System.out.println("쿼리후왓니");
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
//			preparedStatement.setInt(1,1);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("double")) {
						field.setDouble(fieldData, resultSet.getDouble(fieldName));
					} else if(fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					}else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	
	public void selectData2(T t) {
//		try {
//			Class<?> dataClass = t.getClass();
//			Field[] dataClassFields = dataClass.getDeclaredFields();
//			this.dataList = new ArrayList<T>();
		
			if (this.connection == null) {
				this.open();
			}
			String url = "jdbc:oracle:thin:@192.168.110.112:1521:orcl";
	        String sql = "select * from leewon";
	        Connection conn = null;
	        java.sql.Statement st = null; //DB와 소통하는 통로

	        ResultSet rs = null; //결과 받아서 처리할때
	        try {
	            Class.forName("oracle.jdbc.driver.OracleDriver");
	            System.out.println("driver load 성공!");
	            conn = DriverManager.getConnection(url, "kopo", "kopo");
	            System.out.println("DB 연결 성공!");

	            st = conn.createStatement();
	            rs = st.executeQuery(sql); //쿼리 실행 후 데이터들이 rs 저장
	            while(rs.next()){ //한건씩 처리
	                int empid = rs.getInt("IDX"); //첫번째 칼럼 조회
	                String fname = rs.getString("REGIONSEG1"); //컬럼이름도 지정 가능
	                String sal = rs.getString("PRODUCTSEG2"); //컬럼이름도 지정 가능
	                String sal1 = rs.getString("PRODUCTSEG12");
//	                String sal2 = rs.getString("PRODUCTSEG2");
//	                int sal = rs.getInt("salary");
//	                Date hireDate =  rs.getDate("hire_date");
//	                System.out.println(empid+ "\t" + fname + "\t" + sal + "\t" + hireDate);
	                System.out.println(empid + "\t" + fname + "\t" + sal + "\t" + sal1);
	            }
	        } catch (ClassNotFoundException e) {
	            System.out.println("driver load 실패!");
	            e.printStackTrace();
	        } catch (SQLException e) {
	            System.out.println("DB 연결 실패!");
	            e.printStackTrace();
	        } finally {
	            try {
	                if(rs != null) rs.close();
	                if(st != null) st.close();
	                if(conn != null) conn.close();
	            } catch (SQLException e) {
	                e.printStackTrace();
	            }
	            this.close();
	        }
	}
//				this.dataList.add(fieldData);
//			}
//			preparedStatement.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
			
//		}
	

	public ArrayList<T> selectArrayList(T t) {
		this.selectData(t);
		return this.dataList;
	}

	public String selectDataTableTag(T t) {
		this.selectData(t);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}
	
	//jongsun
	public void selectData2(T t, String name) {
		try {
			name="'"+name+"'";
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName + " WHERE name="+name+";";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
//			preparedStatement.setInt(1, 1);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	//jongsun
	public ArrayList<T> selectArrayList2(T t, String name) {
		this.selectData2(t,name);
		return this.dataList;
	}

	//jongsun
	public String selectDataTableTag2(T t, String name) {
		this.selectData2(t,name);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}

	public T detailsData(T t, int idx) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName + " WHERE idx=?;";
			
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			preparedStatement.setInt(1, idx);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		if (this.dataList.size() > 0) {
			return this.dataList.get(0);
		}
		return null;
	}
	
	public HashMap<String, String> detailsData2(T t, int idx) {
		HashMap<String, String> details = new HashMap<String, String>();
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName + " WHERE idx=?;";

			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			preparedStatement.setInt(1, idx);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					details.put(fieldName + "2", resultSet.getString(fieldName));
				}
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		return details;
	}


	public int signIn(String id, String password) {
		int userIdx = 0;
		try {
			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName + " WHERE id='" + id + "' AND password='" + password
					+ "';";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				userIdx = resultSet.getInt("idx");
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		return userIdx;
	}

	public void searchData(T t, String searchFieldName, String searchValue) {
		try {
			Class<?> dataClass = t.getClass();
			Field[] dataClassFields = dataClass.getDeclaredFields();
			this.dataList = new ArrayList<T>();

			if (this.connection == null) {
				this.open();
			}
			String query = "SELECT * FROM " + this.tableName + " WHERE " + searchFieldName + " LIKE '%" + searchValue + "%';";
			PreparedStatement preparedStatement = this.connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T fieldData = (T) dataClass.getDeclaredConstructor().newInstance();
				for (Field field : dataClassFields) {
					String fieldType = field.getType().toString();
					String fieldName = field.getName();
					if (fieldType.matches("int")) {
						field.setInt(fieldData, resultSet.getInt(fieldName));
					} else {
						field.set(fieldData, resultSet.getString(fieldName));
					}
				}
				this.dataList.add(fieldData);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public String searchDataTableTag(T t, String searchFieldName, String searchValue) {
		this.searchData(t, searchFieldName, searchValue);
		Class<?> dataClass = t.getClass();
		String returnString = "";
		for (int i = 0; i < this.dataList.size(); i++) {
			try {
				Method toTableTagStringMethod = dataClass.getDeclaredMethod("toTableTagString");
				returnString = returnString + (String) toTableTagStringMethod.invoke(this.dataList.get(i));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnString;
	}
}
