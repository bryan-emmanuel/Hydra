package com.figsolutions.hydra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HydraService {

	private static int sListenPort = 9001;
	private static int sClientThreadSize = 1;
	private static AcceptThread sAcceptThread;
	private static final String sAlias = "Alias";
	private static final String sType = "Type";
	private static final String sDatabase = "Database";
	private static final String sHostName = "HostName";
	private static final String sHostPort = "HostPort";
	private static final String sUsername = "Username";
	private static final String sPassword = "Password";
	private static final String sConnectionsSize = "ConnectionsSize";
	private static final String sConnectionTimeout = "ConnectionTimeout";
	private static HashMap<String, HashMap<String, String>> sDatabases = new HashMap<String, HashMap<String, String>>();
	private static HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();
	private static byte[] SCipherKey = "figsolutions".getBytes();

	public static void main(String[] args) {

		File f = new File("hydra.conf");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		FileInputStream fis;
		try {
			fis = new FileInputStream("hydra.conf");
			InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
			char[] buffer = new char[1024];
			StringBuilder content = new StringBuilder();
			int read = 0;
			while ((read = isr.read(buffer)) != -1) {
				content.append(buffer, 0, read);
			}
			JSONParser parser = new JSONParser();
			JSONArray conf = (JSONArray) parser.parse(content.toString());
			for (int i = 0, l = conf.size(); i < l; i++) {
				JSONObject databaseIn = (JSONObject) conf.get(i);
				String alias = (String) databaseIn.get(sAlias);
				HashMap<String, String> database = new HashMap<String, String>();
				database.put(sType, (String) databaseIn.get(sType));
				database.put(sDatabase, (String) databaseIn.get(sDatabase));
				database.put(sHostName, (String) databaseIn.get(sHostName));
				database.put(sHostPort, (String) databaseIn.get(sHostPort));
				database.put(sUsername, (String) databaseIn.get(sUsername));
				database.put(sPassword, (String) databaseIn.get(sPassword));
				database.put(sConnectionsSize, (String) databaseIn.get(sConnectionsSize));
				database.put(sConnectionTimeout, (String) databaseIn.get(sConnectionTimeout));
				sDatabases.put(alias, database);
				sDatabaseConnections.put(alias, new ArrayList<DatabaseConnection>());
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//		FileOutputStream fos;
		//		try {
		//			fos = new FileOutputStream("hydra.properties");
		//			OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
		//		} catch (FileNotFoundException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		} catch (UnsupportedEncodingException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}

		//		SCipherKey = "figsolutionssupersecretkey".getBytes("UTF-8");
		//		MessageDigest sha = MessageDigest.getInstance("SHA-1");
		//		SCipherKey = sha.digest(SCipherKey);
		//		SCipherKey = Arrays.copyOf(SCipherKey, 16);
		//		
		//		SecretKeySpec key = new SecretKeySpec(SCipherKey, "AES");
		//		cipher = Cipher.getInstance("AES");
		//	    cipher.init(Cipher.ENCRYPT_MODE, key);

		// listen for connections
		sAcceptThread = new AcceptThread(sListenPort, sClientThreadSize);
		Thread acceptThread = new Thread(sAcceptThread);
		acceptThread.start();
		try {
			acceptThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static int getListenPort() {
		return sListenPort;
	}

	public static void setListenPort(int listenPort) {
		HydraService.sListenPort = listenPort;
	}

	public synchronized static DatabaseConnection getDatabaseConnection(String database) throws Exception {
		// return an existing database connection, or spawn a new one if the pool isn't full
		DatabaseConnection databaseConnection = null;
		// check for existing connection
		if (sDatabaseConnections.containsKey(database)) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			Iterator<DatabaseConnection> iter = connections.iterator();
			while (iter.hasNext()) {
				databaseConnection = iter.next();
				if (databaseConnection.connect()) {
					return databaseConnection;
				}
			}
		}
		// if an existing connection cannot be used
		if (sDatabases.containsKey(database)) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			HashMap<String, String> databaseSettings = sDatabases.get(database);
			if (connections.size() < Integer.parseInt(databaseSettings.get(sConnectionsSize))) {
				String type = databaseSettings.get(sType);
				if (type.equals("unidata")) {
					databaseConnection = new UnidataConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), Long.parseLong(databaseSettings.get(sConnectionTimeout)));
					databaseConnection.connect();
				} else if (type.equals("mssql")) {
					databaseConnection = new MSSQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), Long.parseLong(databaseSettings.get(sConnectionTimeout)));
					databaseConnection.connect();
				} else if (type.equals("oracle")) {
					databaseConnection = new OracleConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), Long.parseLong(databaseSettings.get(sConnectionTimeout)));
					databaseConnection.connect();
				} else if (type.equals("mysql")) {
					databaseConnection = new MySQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), Long.parseLong(databaseSettings.get(sConnectionTimeout)));
					databaseConnection.connect();
				} else if (type.equals("postresql")) {
					databaseConnection = new PostgreSQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), Long.parseLong(databaseSettings.get(sConnectionTimeout)));
					databaseConnection.connect();
				} else {
					throw new Exception("unknown type:" + type);
				}
				connections.add(databaseConnection);
				return databaseConnection;
			}
		} else {
			throw new Exception("Database " + database + " not defined.");
		}
		return databaseConnection;
	}

	public static boolean pendingConnections() {
		return (sAcceptThread.getClientThreads() >= sDatabaseConnections.size());
	}

	public synchronized static void removeClientThread(int index) {
		//TODO: when should database connections be cleaned up? timeout which is set in the connect() method
		if (sAcceptThread.getClientThreads() > index) {
			sAcceptThread.removeClientThread(index);
		}
	}
	
	@SuppressWarnings("unchecked")
	public synchronized static String getDatabases() {
		JSONObject response = new JSONObject();
		Set<String> databases = sDatabases.keySet();
		JSONArray arr = new JSONArray();
		Iterator<String> iter = databases.iterator();
		while(iter.hasNext()) {
			arr.add(iter.next());
		}
		response.put("result", arr);
		return response.toString();
	}
	
	@SuppressWarnings("unchecked")
	public synchronized static String getDatabase(String database) {
		JSONObject response = new JSONObject();
		if (sDatabases.containsKey(database)) {
			HashMap<String, String> databaseSettings = sDatabases.get(database);
			response.put(sAlias, database);
			response.put(sDatabase, databaseSettings.get(sDatabase));
			response.put(sHostName, databaseSettings.get(sHostName));
			response.put(sHostPort, databaseSettings.get(sHostPort));
		}
		return response.toString();
	}
}
