/*
 * Hydra
 * Copyright (C) 2012 Bryan Emmanuel
 * 
 * This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Bryan Emmanuel piusvelte@gmail.com
 */
package com.piusvelte.hydra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class HydraService extends Thread {

	private AcceptThread sAcceptThread;
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
	protected static final String DB_TYPE_UNIDATA = "unidata";
	protected static final String DB_TYPE_MYSQL = "mysql";
	protected static final String DB_TYPE_MSSQL = "mssql";
	protected static final String DB_TYPE_ORACLE = "oracle";
	protected static final String DB_TYPE_POSTGRESQL = "postgresql";
	private String sConnectionPassphrase;
	private String sSalt;
	private String sLogFile = "hydra.log";
	protected FileHandler sLogFileHandler;
	protected Logger sLogger;
	private HashMap<String, HashMap<String, String>> sDatabaseSettings = new HashMap<String, HashMap<String, String>>();
	private HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();
	private String sQueueFileName = "queue.txt";
	private int[] sQueueLock = new int[0];// small lock object
	private String sHydraProperties = "hydra.properties";
	private QueueThread sQueueThread = null;
	private int mQueueRetryInterval;
	private boolean mKeepAlive = true;

	@Override
	public void run() {
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

		File f = new File(sHydraProperties);
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
			int connections = AcceptThread.DEFAULT_CONNECTIONS; // default unlimited
			mQueueRetryInterval = QueueThread.DEFAULT_QUEUERETRYINTERVAL;

			FileInputStream fis = null;
			try {
				fis = new FileInputStream(f);
			} catch (FileNotFoundException e) {
				writeLog(e.getMessage());
				fis = null;
			}
			if (fis != null) {
				Properties properties = new Properties();
				try {
					properties.load(fis);
				} catch (IOException e) {
					properties = null;
					e.printStackTrace();
				}
				if (properties != null) {
					sConnectionPassphrase = properties.getProperty(sPassphrase);
					if (properties.containsKey(sPort))
						listenPort = Integer.parseInt(properties.getProperty(sPort));
					if (properties.containsKey(sConnections))
						connections = Integer.parseInt(properties.getProperty(sConnections));
					if (properties.containsKey(sQueueRetryInterval))
						mQueueRetryInterval = Integer.parseInt(properties.getProperty(sQueueRetryInterval));
					if (properties.containsKey(sDatabases)) {
						String[] databaseAliases = properties.getProperty(sDatabases).split(",");
						String[] databaseProperties = new String[]{sType, sDatabase, sHost, sPort, sUsername, sPassword, sConnections, sDASU, sDASP, sSQLENVINIT};
						for (String databaseAlias : databaseAliases) {
							HashMap<String, String> database = new HashMap<String, String>();
							for (String databaseProperty : databaseProperties)
								database.put(databaseProperty, properties.getProperty(databaseAlias + "." + databaseProperty, ""));
							sDatabaseSettings.put(databaseAlias, database);
							sDatabaseConnections.put(databaseAlias, new ArrayList<DatabaseConnection>());
						}
					}
					try {
						fis.close();
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

					startQueueThread();

					// listen for connections
					(sAcceptThread = new AcceptThread(this, listenPort, connections, sConnectionPassphrase, sSalt)).start();
					while (mKeepAlive);
					if (sQueueThread != null)
						stopQueueThread();
					sAcceptThread.shutdown();
				} else
					System.out.println("unable to load " + sHydraProperties);
			} else
				System.out.println("unable to read " + sHydraProperties);
		} else
			System.out.println("unable to create " + sHydraProperties);
	}
	
	public void shutdown() {
		mKeepAlive = false;
	}
	
	public boolean isShutdown() {
		return !mKeepAlive;
	}

	protected synchronized void writeLog(String message) {
		if ((sLogFileHandler != null) && (sLogger != null))
			sLogger.info(message);
		else
			System.out.println(message);
	}

	public synchronized DatabaseConnection getDatabaseConnection(String database) throws Exception {
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
			writeLog("get new connection to: " + database);
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			HashMap<String, String> databaseSettings = sDatabaseSettings.get(database);
			if (connections.size() < Integer.parseInt(databaseSettings.get(sConnections))) {
				String type = databaseSettings.get(sType);
				int port = Integer.parseInt(databaseSettings.get(sPort));
				if (DB_TYPE_UNIDATA.equals(type))
					(databaseConnection = new UnidataConnection(this, databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword), databaseSettings.get(sDASU), databaseSettings.get(sDASP), databaseSettings.get(sSQLENVINIT))).connect();
				else if (DB_TYPE_MSSQL.equals(type))
					(databaseConnection = new MSSQLConnection(this, databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_ORACLE.equals(type))
					(databaseConnection = new OracleConnection(this, databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_MYSQL.equals(type))
					(databaseConnection = new MySQLConnection(this, databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
				else if (DB_TYPE_POSTGRESQL.equals(type))
					(databaseConnection = new PostgreSQLConnection(this, databaseSettings.get(sHost), port, databaseSettings.get(sDatabase), databaseSettings.get(sUsername), databaseSettings.get(sPassword))).connect();
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

	public synchronized void removeClientThread(int index) {
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
	public synchronized JSONObject getDatabases() {
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
	public synchronized JSONObject getDatabase(String database) {
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

	protected String getHashString(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
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

	protected static File getFile(String name) {
		File f = new File(name);
		if (!f.exists()) {
			try {
				if (!f.createNewFile())
					f = null;
			} catch (IOException e) {
				f = null;
				e.printStackTrace();
			}
		}
		return f;
	}

	protected boolean queueRequest(String request) {
		boolean queued = false;
		synchronized (sQueueLock) {
			File queueFile = getFile(sQueueFileName);
			if (queueFile != null) {
				FileWriter fw = null;
				try {
					fw = new FileWriter(queueFile, true);
				} catch (IOException e) {
					fw = null;
					e.printStackTrace();
				}
				if (fw != null) {
					PrintWriter pw = new PrintWriter(fw);
					pw.println(request);
					pw.close();
					queued = true;
					try {
						fw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			writeLog("queue: " + request);
		}
		startQueueThread();
		return queued;
	}

	protected String dequeueRequest() {
		synchronized (sQueueLock) {
			File queueFile = getFile(sQueueFileName);
			if (queueFile != null) {
				FileReader fr = null;
				try {
					fr = new FileReader(queueFile);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				if (fr != null) {
					BufferedReader br = new BufferedReader(fr);
					String request = null;
					try {
						request = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						fr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return request;
				} else
					return null;
			} else
				return null;
		}
	}

	protected void requeueRequest(String request) {
		ArrayList<String> requests = new ArrayList<String>();
		synchronized (sQueueLock) {
			File queueFile = getFile(sQueueFileName);
			if (queueFile != null) {
				FileReader fr = null;
				try {
					fr = new FileReader(queueFile);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				if (fr != null) {
					BufferedReader br = new BufferedReader(fr);
					String r = null;
					try {
						// skip the first line as it's been processed
						br.readLine();
						r = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					while (r != null) {
						requests.add(r);
						try {
							r = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					if (request != null)
						requests.add(request);
					if (requests.isEmpty())
						queueFile.delete();
					else {
						FileWriter fw = null;
						try {
							fw = new FileWriter(queueFile);
						} catch (IOException e) {
							fw = null;
							e.printStackTrace();
						}
						if (fw != null) {
							PrintWriter pw = new PrintWriter(fw);
							for (String s : requests)
								pw.println(s);
							pw.close();
							try {
								fw.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						fr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	protected void startQueueThread() {
		synchronized (sQueueLock) {
			// start the queue thread
			if (sQueueThread == null) {
				sQueueThread = new QueueThread(this, mQueueRetryInterval);
				System.out.println("***START QUEUE THREAD***");
				sQueueThread.start();
			}
		}
	}

	protected boolean stopQueueThread() {
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