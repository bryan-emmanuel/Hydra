package com.figsolutions.hydra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

	private static AcceptThread sAcceptThread;
	private static final String sPassphrase = "passphrase";
	private static final String sDatabases = "databases";
	private static final String sAlias = "alias";
	private static final String sType = "type";
	private static final String sDatabase = "database";
	private static final String sHost = "host";
	private static final String sPort = "port";
	private static final String sUsername = "username";
	private static final String sPassword = "password";
	private static final String sConnections = "connections";
	private static final String sQueueRetryInterval = "queueretryinterval";
	private static final String sDASU = "DASU";
	private static final String sDASP = "DASP";
	private static final String sSQLENVINIT = "SQLENVINIT";
	private static String sConnectionPassphrase;
	private static String sSalt;
	private static final String sLogFile = "hydra.log";
	protected static FileHandler sLogFileHandler;
	protected static Logger sLogger;
	private static HashMap<String, HashMap<String, String>> sDatabaseSettings = new HashMap<String, HashMap<String, String>>();
	private static HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();
	protected static final String DB_TYPE_UNIDATA = "unidata";
	protected static final String DB_TYPE_MYSQL = "mysql";
	protected static final String DB_TYPE_MSSQL = "mssql";
	protected static final String DB_TYPE_ORACLE = "oracle";
	protected static final String DB_TYPE_POSTGRESQL = "postgresql";
	private static final String sQueueFileName = "queue.txt";
	private static File sQueueFile = null;
	private static int[] sQueueLock = new int[0];// small lock object
	private static final String sHydraConfFileName = "hydra.conf";
	private static ArrayList<String> sQueue = new ArrayList<String>();
	private static QueueThread sQueueThread = null;
	private static int mQueueRetryInterval;

	public static void main(String[] args) {

		try {
			sLogFileHandler = new FileHandler(sLogFile);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (sLogFileHandler != null) {
			sLogger = Logger.getLogger("Hydra");
			sLogger.addHandler(sLogFileHandler);
			SimpleFormatter sf = new SimpleFormatter();
			sLogFileHandler.setFormatter(sf);
			writeLog("Hydra starting");
		}

		File f = new File(sHydraConfFileName);
		if (!f.exists()) {
			try {
				if (!f.createNewFile())
					f = null;
			} catch (IOException e) {
				writeLog(e.getMessage());
			}
		}

		if (f != null) {
			int listenPort = 9001;
			int connections = AcceptThread.DEFAULT_CONNECTIONS; // default unlimiteds
			mQueueRetryInterval = QueueThread.DEFAULT_QUEUERETRYINTERVAL;
			FileInputStream fis;
			try {
				writeLog("parsing conf file");
				fis = new FileInputStream(sHydraConfFileName);
				InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
				char[] buffer = new char[1024];
				StringBuilder content = new StringBuilder();
				int read = 0;
				while ((read = isr.read(buffer)) != -1)
					content.append(buffer, 0, read);
				JSONParser parser = new JSONParser();
				JSONObject conf = (JSONObject) parser.parse(content.toString());
				sConnectionPassphrase = (String) conf.get(sPassphrase);
				if (conf.containsKey(sPort))
					listenPort = Integer.parseInt((String) conf.get(sPort));
				if (conf.containsKey(sConnections))
					connections = Integer.parseInt((String) conf.get(sConnections));
				if (conf.containsKey(sQueueRetryInterval))
					mQueueRetryInterval = Integer.parseInt((String) conf.get(sQueueRetryInterval));
				JSONArray databases = (JSONArray) conf.get(sDatabases);
				for (int i = 0, l = databases.size(); i < l; i++) {
					JSONObject databaseIn = (JSONObject) databases.get(i);
					String alias = (String) databaseIn.get(sAlias);
					HashMap<String, String> database = new HashMap<String, String>();
					database.put(sType, (String) databaseIn.get(sType));
					database.put(sDatabase, (String) databaseIn.get(sDatabase));
					database.put(sHost, (String) databaseIn.get(sHost));
					database.put(sPort, (String) databaseIn.get(sPort));
					database.put(sUsername, (String) databaseIn.get(sUsername));
					database.put(sPassword, (String) databaseIn.get(sPassword));
					database.put(sConnections, (String) databaseIn.get(sConnections));
					database.put(sDASU, (String) databaseIn.get(sDASU));
					database.put(sDASP, (String) databaseIn.get(sDASP));
					database.put(sSQLENVINIT, (String) databaseIn.get(sSQLENVINIT));
					sDatabaseSettings.put(alias, database);
					sDatabaseConnections.put(alias, new ArrayList<DatabaseConnection>());
				}
			} catch (FileNotFoundException e1) {
				writeLog(e1.getMessage());
			} catch (UnsupportedEncodingException e) {
				writeLog(e.getMessage());
			} catch (ParseException e) {
				writeLog(e.getMessage());
			} catch (IOException e) {
				writeLog(e.getMessage());
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

			// check the queue
			
			if (readQueue()) {
				sQueueThread = new QueueThread(mQueueRetryInterval);
				sQueueThread.start();
			}
			
			// listen for connections
			sAcceptThread = new AcceptThread(listenPort, connections, sConnectionPassphrase, sSalt);
			sAcceptThread.start();
			try {
				sAcceptThread.join(); // blocking method
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (sQueueThread != null)
				stopQueueThread();
			sAcceptThread.shutdown();
			writeQueue();
		} else
			System.out.println("unable to read " + sHydraConfFileName);
	}

	protected static void writeLog(String message) {
		if ((sLogFileHandler != null) && (sLogger != null))
			sLogger.info(message);
		else
			System.out.println(message);
	}

	public synchronized static DatabaseConnection getDatabaseConnection(String database) throws Exception {
		// return an existing database connection, or spawn a new one if the pool isn't full
		// check for existing connection
		if (sDatabaseConnections.containsKey(database)) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			for (DatabaseConnection databaseConnection : connections) {
				if (databaseConnection.connect())
					return databaseConnection;
			}
		}
		DatabaseConnection databaseConnection = null;
		// if an existing connection cannot be used
		if (sDatabaseSettings.containsKey(database)) {
			HydraService.writeLog("get new connection to: " + database);
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			HashMap<String, String> databaseSettings = sDatabaseSettings.get(database);
			if (connections.size() < Integer.parseInt(databaseSettings.get(sConnections))) {
				String type = databaseSettings.get(sType);
				int port = Integer.parseInt(databaseSettings.get(sPort));
				if (DB_TYPE_UNIDATA.equals(type))
					(databaseConnection = new UnidataConnection(databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), databaseSettings.get(sDASU), databaseSettings.get(sDASP), databaseSettings.get(sSQLENVINIT))).connect();
				else if (DB_TYPE_MSSQL.equals(type))
					(databaseConnection = new MSSQLConnection(databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_ORACLE.equals(type))
					(databaseConnection = new OracleConnection(databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_MYSQL.equals(type))
					(databaseConnection = new MySQLConnection(databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_POSTGRESQL.equals(type))
					(databaseConnection = new PostgreSQLConnection(databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else
					throw new Exception("unknown type:" + type);
				connections.add(databaseConnection);
				sDatabaseConnections.put(database, connections);
				return databaseConnection;
			}
		} else
			throw new Exception("Database " + database + " not defined.");
		return databaseConnection;
	}

	public synchronized static void removeClientThread(int index) {
		int clients = sAcceptThread.removeClientThread(index);
		// maintain atleast as many connections/database as client threads
		for (String key : sDatabaseConnections.keySet()) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(key);
			// clean up unnecessary connections
			for (int i = 0; (i < connections.size()) && (connections.size() > clients); i++) {
				DatabaseConnection connection = connections.get(i);
				if (!connection.isLocked()) {
					writeLog("disconnect database");
					try {
						connection.disconnect();
					} catch (Exception e) {
						writeLog(e.getMessage());
					}
					// remove the connection and back up the index
					connections.remove(i--);
				}
			}
			sDatabaseConnections.put(key, connections);
		}

	}

	@SuppressWarnings("unchecked")
	public synchronized static JSONObject getDatabases() {
		JSONObject response = new JSONObject();
		Set<String> databases = sDatabaseSettings.keySet();
		JSONArray arr = new JSONArray();
		Iterator<String> iter = databases.iterator();
		while (iter.hasNext())
			arr.add(iter.next());
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
			props.put(sHost, databaseSettings.get(sHost));
			props.put(sPort, databaseSettings.get(sPort));
			props.put(sType, databaseSettings.get(sType));
			response.put("result", props);
		}
		return response;
	}

	protected static String getHashString(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(str.getBytes("UTF-8"));
		StringBuffer hexString = new StringBuffer();
		byte[] hash = md.digest();
		for (byte b : hash) {
			if ((0xFF & b) < 0x10)
				hexString.append("0" + Integer.toHexString((0xFF & b)));
			else
				hexString.append(Integer.toHexString(0xFF & b));
		}
		return hexString.toString();
	}

	// request queueing
	// this is in case a database connection becomes unavailable
	protected static void writeQueue() {
		synchronized (sQueueLock) {
			if (!sQueue.isEmpty()) {
				if (sQueueFile == null) {
					sQueueFile = new File(sQueueFileName);
					if (!sQueueFile.exists()) {
						try {
							if (!sQueueFile.createNewFile())
								sQueueFile = null;
						} catch (IOException e) {
							sQueueFile = null;
							e.printStackTrace();
						}
					} else
						sQueueFile.delete(); // avoid a phantom queue
				}
				if (sQueueFile != null) {
					FileWriter fw = null;
					try {
						fw = new FileWriter(sQueueFile, true);
					} catch (IOException e) {
						fw = null;
						e.printStackTrace();
					}
					if (fw != null) {
						PrintWriter pw = new PrintWriter(fw);
						for (String s : sQueue) {
							pw.println(s);
							writeLog("write queue: " + s);
						}
						pw.close();
						try {
							fw.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	protected static void queueRequest(String request) {
		synchronized (sQueueLock) {
			sQueue.add(request);
			writeLog("queue: " + request);
			// start the queue thread
			if (sQueueThread == null) {
				sQueueThread = new QueueThread(mQueueRetryInterval);
				sQueueThread.start();
			}
		}
	}

	protected static boolean readQueue() {
		if (sQueueFile == null) {
			sQueueFile = new File(sQueueFileName);
			if (!sQueueFile.exists())
				sQueueFile = null;
		}
		if (sQueueFile != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(sQueueFile);
			} catch (FileNotFoundException e) {
				fis = null;
				e.printStackTrace();
			}
			if (fis != null) {
				InputStreamReader isr = null;
				try {
					isr = new InputStreamReader(fis, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					isr = null;
					e.printStackTrace();
				}
				if (isr != null) {
					BufferedReader br = new BufferedReader(isr);
					String request = null;
					try {
						request = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					while (request != null) {
						writeLog("read queue: " + request);
						synchronized (sQueueLock) {
							sQueue.add(request);
						}
						try {
							request = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		return hasQueue();
	}
	
	protected static boolean hasQueue() {
		synchronized (sQueueLock) {
			return !sQueue.isEmpty();
		}
	}
	
	protected static ArrayList<String> getQueue() {
		synchronized (sQueueLock) {
			ArrayList<String> queue = sQueue;
			sQueue.clear();
			return queue;
		}
	}
	
	protected static boolean stopQueueThread() {
		synchronized (sQueueLock) {
			if (sQueueThread != null) {
				sQueueThread.shutdown();
				sQueueThread = null;
				return true;
			} else
				return false;
		}
	}
}