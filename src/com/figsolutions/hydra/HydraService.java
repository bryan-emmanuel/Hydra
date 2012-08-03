package com.figsolutions.hydra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// windows service setup:
// requires Windows Resource Kit
// sc create Hydra binPath= "C:\Program Files\Windows Resource Kits\Tools\srvany.exe"
// edit:
// HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Hydra
// add registry key:
//  Parameters
// add string values:
//  AppDirectory - full path to Hydra
//  Application - full path to java.exe
//  AppParameters -jar Hydra.jar

public class HydraService {

	private static int sListenPort = 9001;
	private static int sClientThreadSize = 1;
	private static AcceptThread sAcceptThread;
	private static final String sPassphrase = "passphrase";
	private static final String sDatabases = "databases";
	private static final String sAlias = "alias";
	private static final String sType = "type";
	private static final String sDatabase = "database";
	private static final String sHostName = "hostname";
	private static final String sHostPort = "hostport";
	private static final String sUsername = "username";
	private static final String sPassword = "password";
	private static final String sConnections = "connections";
	private static String sConnectionPassphrase;
	private static String sSalt;
	private static HashMap<String, HashMap<String, String>> sDatabaseSettings = new HashMap<String, HashMap<String, String>>();
	private static HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();

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
			JSONObject conf = (JSONObject) parser.parse(content.toString());
			sConnectionPassphrase = (String) conf.get(sPassphrase);
			JSONArray databases = (JSONArray) conf.get(sDatabases);
			for (int i = 0, l = databases.size(); i < l; i++) {
				JSONObject databaseIn = (JSONObject) databases.get(i);
				String alias = (String) databaseIn.get(sAlias);
				HashMap<String, String> database = new HashMap<String, String>();
				database.put(sType, (String) databaseIn.get(sType));
				database.put(sDatabase, (String) databaseIn.get(sDatabase));
				database.put(sHostName, (String) databaseIn.get(sHostName));
				database.put(sHostPort, (String) databaseIn.get(sHostPort));
				database.put(sUsername, (String) databaseIn.get(sUsername));
				database.put(sPassword, (String) databaseIn.get(sPassword));
				database.put(sConnections, (String) databaseIn.get(sConnections));
				sDatabaseSettings.put(alias, database);
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
		
		// generate a salt
		String salt = new BigInteger(256, new SecureRandom()).toString(16);
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			md.update(salt.getBytes("UTF-8"));
			sSalt = new BigInteger(1, md.digest()).toString(16);
			if (sSalt.length() > 64) {
				sSalt = sSalt.substring(0, 64);
			}
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		// for writing a conf file...
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

		// listen for connections
		sAcceptThread = new AcceptThread(sListenPort, sClientThreadSize, sConnectionPassphrase, sSalt);
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
		if (sDatabaseSettings.containsKey(database)) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			HashMap<String, String> databaseSettings = sDatabaseSettings.get(database);
			if (connections.size() < Integer.parseInt(databaseSettings.get(sConnections))) {
				String type = databaseSettings.get(sType);
				if (type.equals("unidata")) {
					databaseConnection = new UnidataConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword));
					databaseConnection.connect();
				} else if (type.equals("mssql")) {
					databaseConnection = new MSSQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword));
					databaseConnection.connect();
				} else if (type.equals("oracle")) {
					databaseConnection = new OracleConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword));
					databaseConnection.connect();
				} else if (type.equals("mysql")) {
					databaseConnection = new MySQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword));
					databaseConnection.connect();
				} else if (type.equals("postresql")) {
					databaseConnection = new PostgreSQLConnection(databaseSettings.get(sHostName), databaseSettings.get(sHostPort), databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword));
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

	public synchronized static void removeClientThread(int index) {
		int clients = sAcceptThread.getClientThreads();
		if (clients > index) {
			sAcceptThread.removeClientThread(index);
		}
		// maintain atleast as many connections/database as client threads
		Set<String> keys = sDatabaseConnections.keySet();
		Iterator<String> iter = keys.iterator();
		while (iter.hasNext()) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(iter.next());
			// clean up unnecessary connections
			for (int i = 0; (i < connections.size()) && (connections.size() > clients); i++) {
				DatabaseConnection connection = connections.get(i);
				if (!connection.isLocked()) {
					try {
						connection.disconnect();
					} catch (Exception e) {
						e.printStackTrace();
					}
					// remove the connection and back up the index
					connections.remove(i);
					i--;
				}
			}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public synchronized static JSONObject getDatabases() {
		JSONObject response = new JSONObject();
		Set<String> databases = sDatabaseSettings.keySet();
		JSONArray arr = new JSONArray();
		Iterator<String> iter = databases.iterator();
		while (iter.hasNext()) {
			arr.add(iter.next());
		}
		response.put("result", arr);
		return response;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized static JSONObject getDatabase(String database) {
		JSONObject response = new JSONObject();
		if (sDatabaseSettings.containsKey(database)) {
			HashMap<String, String> databaseSettings = sDatabaseSettings.get(database);
			JSONObject props = new JSONObject();
			props.put(sAlias, database);
			props.put(sDatabase, databaseSettings.get(sDatabase));
			props.put(sHostName, databaseSettings.get(sHostName));
			props.put(sHostPort, databaseSettings.get(sHostPort));
			response.put("result", props);
		}
		return response;
	}
}
