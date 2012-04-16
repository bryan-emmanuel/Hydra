package com.figsolutions.hydra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class MySQLConnection extends DatabaseConnection {
	
	Connection mConnection;
	private static final String SIMPLE_QUERY_FORMAT = "SELECT %s FROM %s";
	private static final String SELECTION_QUERY_FORMAT = "SELECT %s FROM %s WHERE %s";
	private static final String INSERT_QUERY = "INSERT INTO %s (%s) VALUES (%s)";
	private static final String UPDATE_QUERY = "UPDATE %s SET %s WHERE %s";
	private static final String DELETE_QUERY = "DELETE FROM %s WHERE %s";

	public MySQLConnection(String hostName, String hostPort, String accountPath, String username, String password, long timeout) {
		super(hostName, hostPort, accountPath, username, password, timeout);
	}

	@Override
	public boolean connect() throws Exception {
		super.connect();
		if (mConnection == null) {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			StringBuilder connectionString = new StringBuilder();
			connectionString.append("jdbc:mysql://");
			connectionString.append(mHostName);
			connectionString.append(":");
			connectionString.append(mHostPort);
			mConnection = DriverManager.getConnection(connectionString.toString(), mUsername, mPassword);
		}
		return mLock;
	}

	@Override
	public void disconnect() throws Exception {
		super.disconnect();
		if ((mConnection != null) && !HydraService.pendingConnections()) {
			mConnection.close();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String execute(String statement) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		Statement s = null;
		ResultSet rs = null;
		try {
			s = mConnection.createStatement();
			rs = s.executeQuery(statement);
			JSONArray result = new JSONArray();
			ResultSetMetaData rsmd = rs.getMetaData();
			String[] columnsArr = new String[rsmd.getColumnCount()];
			for (int c = 0, l = columnsArr.length; c < l; c++) {
				columnsArr[c] = rsmd.getColumnName(c);
			}
			while (rs.next()) {
				for (String column : columnsArr) {
					JSONObject col = new JSONObject();
					col.put(column, (String) rs.getObject(column));
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (SQLException e) {
			errors.add(e.getMessage());
			e.printStackTrace();
		} finally {
			if (s != null) {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						errors.add(e.getMessage());
						e.printStackTrace();
					}
				}
				try {
					s.close();
				} catch (SQLException e) {
					errors.add(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		response.put("errors", errors);
		return response.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String query(String object, String[] columns, String selection) {
		Statement s = null;
		ResultSet rs = null;
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		try {
			StringBuilder sb = new StringBuilder();
			for (int i = 0, l = columns.length; i < l; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(columns[i]);
			}
			String columnsStr = sb.toString();
			s = mConnection.createStatement();
			if (selection != null) {
				rs = s.executeQuery(String.format(SELECTION_QUERY_FORMAT, columnsStr, object, selection).toString());
			} else {
				rs = s.executeQuery(String.format(SIMPLE_QUERY_FORMAT, columnsStr, object).toString());
			}
			JSONArray result = new JSONArray();
			while (rs.next()) {
				for (String column : columns) {
					JSONObject col = new JSONObject();
					col.put(column, (String) rs.getObject(column));
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (SQLException e) {
			errors.add(e.getMessage());
			e.printStackTrace();
		} finally {
			if (s != null) {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						errors.add(e.getMessage());
						e.printStackTrace();
					}
				}
				try {
					s.close();
				} catch (SQLException e) {
					errors.add(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		response.put("errors", errors);
		return response.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String insert(String object, String[] columns, String[] values) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		Statement s = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			for (int i = 0, l = columns.length; i < l; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(columns[i]);
			}
			String columnsStr = sb.toString();
			sb = new StringBuilder();
			for (int i = 0, l = values.length; i < l; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(values[i]);
			}
			String valuesStr = sb.toString();
			s = mConnection.createStatement();
			rs = s.executeQuery(String.format(INSERT_QUERY, object, columnsStr, valuesStr).toString());
			JSONArray result = new JSONArray();
			ResultSetMetaData rsmd = rs.getMetaData();
			String[] columnsArr = new String[rsmd.getColumnCount()];
			for (int c = 0, l = columnsArr.length; c < l; c++) {
				columnsArr[c] = rsmd.getColumnName(c);
			}
			while (rs.next()) {
				for (String column : columnsArr) {
					JSONObject col = new JSONObject();
					col.put(column, (String) rs.getObject(column));
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (SQLException e) {
			errors.add(e.getMessage());
			e.printStackTrace();
		} finally {
			if (s != null) {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						errors.add(e.getMessage());
						e.printStackTrace();
					}
				}
				try {
					s.close();
				} catch (SQLException e) {
					errors.add(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		response.put("errors", errors);
		return response.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String update(String object, String[] columns, String[] values, String selection) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		Statement s = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			for (int i = 0, l = columns.length; i < l; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(columns[i]);
				sb.append("=");
				sb.append(values[i]);
			}
			s = mConnection.createStatement();
			rs = s.executeQuery(String.format(UPDATE_QUERY, object, sb.toString(), selection).toString());
			JSONArray result = new JSONArray();
			ResultSetMetaData rsmd = rs.getMetaData();
			String[] columnsArr = new String[rsmd.getColumnCount()];
			for (int c = 0, l = columnsArr.length; c < l; c++) {
				columnsArr[c] = rsmd.getColumnName(c);
			}
			while (rs.next()) {
				for (String column : columnsArr) {
					JSONObject col = new JSONObject();
					col.put(column, (String) rs.getObject(column));
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (SQLException e) {
			errors.add(e.getMessage());
			e.printStackTrace();
		} finally {
			if (s != null) {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						errors.add(e.getMessage());
						e.printStackTrace();
					}
				}
				try {
					s.close();
				} catch (SQLException e) {
					errors.add(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		response.put("errors", errors);
		return response.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String delete(String object, String selection) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		Statement s = null;
		ResultSet rs = null;
		try {
			s = mConnection.createStatement();
			rs = s.executeQuery(String.format(DELETE_QUERY, object, selection).toString());
			JSONArray result = new JSONArray();
			ResultSetMetaData rsmd = rs.getMetaData();
			String[] columnsArr = new String[rsmd.getColumnCount()];
			for (int c = 0, l = columnsArr.length; c < l; c++) {
				columnsArr[c] = rsmd.getColumnName(c);
			}
			while (rs.next()) {
				for (String column : columnsArr) {
					JSONObject col = new JSONObject();
					col.put(column, (String) rs.getObject(column));
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (SQLException e) {
			errors.add(e.getMessage());
			e.printStackTrace();
		} finally {
			if (s != null) {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						errors.add(e.getMessage());
						e.printStackTrace();
					}
				}
				try {
					s.close();
				} catch (SQLException e) {
					errors.add(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		response.put("errors", errors);
		return response.toString();
	}

}
