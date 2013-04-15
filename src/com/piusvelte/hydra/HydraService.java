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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletContext;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class HydraService {

	private static final String sPassphrase = "passphrase";
	private static final String sQueue = "queue";
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
	private static String passphrase = null;
	private static int[] databaseLock = new int[0];
	private static HashMap<String, HashMap<String, String>> sDatabaseSettings = new HashMap<String, HashMap<String, String>>();
	private static HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();
	private static HashMap<String, Integer> queuedDatabaseRequests = new HashMap<String, Integer>();
	private static String sQueueFileName = null;
	private static int[] sQueueLock = new int[0];
	private static String sHydraProperties = "/WEB-INF/hydra.properties";
	private static QueueThread sQueueThread = null;
	private static int mQueueRetryInterval;

	private static HydraService hydraService = null;

	private HydraService(ServletContext ctx) {

		mQueueRetryInterval = QueueThread.DEFAULT_QUEUERETRYINTERVAL;

		Properties properties = new Properties();
		try {
			properties.load(ctx.getResourceAsStream(sHydraProperties));
		} catch (IOException e) {
			properties = null;
			e.printStackTrace();
		}

		if (properties != null) {
			if (properties.containsKey(sPassphrase))
				passphrase = properties.getProperty(sPassphrase);
			if (properties.containsKey(sQueue)) {
				sQueueFileName = properties.getProperty(sQueue);
				if (sQueueFileName.length() > 0) {
					if (!File.pathSeparator.equals(sQueueFileName.substring(sQueueFileName.length() - 1, sQueueFileName.length())))
						sQueueFileName += File.pathSeparator;
					sQueueFileName += "queue.log";
				} else
					sQueueFileName = null;
			}
			if (properties.containsKey(sQueueRetryInterval))
				mQueueRetryInterval = Integer.parseInt(properties.getProperty(sQueueRetryInterval));
			if (properties.containsKey(sDatabases)) {
				String[] databaseAliases = properties.getProperty(sDatabases).split(",");
				String[] databaseProperties = new String[]{sType, sDatabase, sHost, sPort, sUsername, sPassword, sConnections, sDASU, sDASP, sSQLENVINIT};
				for (String databaseAlias : databaseAliases) {
					HashMap<String, String> database = new HashMap<String, String>();
					for (String databaseProperty : databaseProperties)
						database.put(databaseProperty, properties.getProperty(databaseAlias + "." + databaseProperty, ""));
					synchronized (databaseLock) {
						sDatabaseSettings.put(databaseAlias, database);
						sDatabaseConnections.put(databaseAlias, new ArrayList<DatabaseConnection>());
						queuedDatabaseRequests.put(databaseAlias, 0);
					}
				}
			}
		}
	}

	public static HydraService getService(ServletContext ctx) {
		if (hydraService == null) {
			hydraService = new HydraService(ctx);
			if (sQueueFileName != null)
				hydraService.startQueueThread();
		}
		return hydraService;
	}

	void shutdown() {
		if (sQueueThread != null)
			stopQueueThread();
	}

	DatabaseConnection getDatabaseConnection(String database) throws Exception {
		// return an existing database connection, or spawn a new one if the pool isn't full
		// check for existing connection
		synchronized (databaseLock) {
			if (sDatabaseConnections.containsKey(database)) {
				ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
				for (DatabaseConnection databaseConnection : connections) {
					if (databaseConnection.connect())
						return databaseConnection;
				}
			}
		}
		DatabaseConnection databaseConnection = null;
		synchronized (databaseLock) {
			if (sDatabaseSettings.containsKey(database)) {
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
		}
		return databaseConnection;
	}

	void queueDatabaseRequest(String database) {
		synchronized (databaseLock) {
			queuedDatabaseRequests.put(database, queuedDatabaseRequests.get(database) + 1);
		}
	}

	void dequeueDatabaseRequest(String database) {
		synchronized (databaseLock) {
			queuedDatabaseRequests.put(database, queuedDatabaseRequests.get(database) - 1);
		}
	}

	void cleanDatabaseConnections(String database) {
		synchronized (databaseLock) {
			ArrayList<DatabaseConnection> connections = sDatabaseConnections.get(database);
			for (int i = 0; (i < connections.size()) && (connections.size() > queuedDatabaseRequests.get(database)); i++) {
				DatabaseConnection connection = connections.get(i);
				if (!connection.isLocked()) {
					try {
						connection.disconnect();
					} catch (Exception e) {
						e.printStackTrace();
					}
					connections.remove(i--);
				}
			}
			sDatabaseConnections.put(database, connections);
		}
	}

	@SuppressWarnings("unchecked")
	JSONObject getDatabases() {
		JSONObject response = new JSONObject();
		synchronized (databaseLock) {
			Set<String> databases = sDatabaseSettings.keySet();
			JSONArray arr = new JSONArray();
			Iterator<String> iter = databases.iterator();
			while (iter.hasNext())
				arr.add(iter.next());
			response.put("result", arr);
		}
		return response;
	}

	@SuppressWarnings("unchecked")
	JSONObject getDatabase(String database) {
		JSONObject response = new JSONObject();
		synchronized (databaseLock) {
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
		}
		return response;
	}

	static String getHash64(String in) {
		String out = null;
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			md.update(in.getBytes("UTF-8"));
			out = new BigInteger(1, md.digest()).toString(16);
			StringBuffer hexString = new StringBuffer();
			byte[] hash = md.digest();
			for (byte b : hash) {
				if ((0xFF & b) < 0x10)
					hexString.append("0" + Integer.toHexString((0xFF & b)));
				else
					hexString.append(Integer.toHexString(0xFF & b));
			}
			out = hexString.toString();
			if (out.length() > 64)
				return out.substring(0, 64);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return out;
	}

	public boolean isAuthenticated(String hmac, String requestAuth) {
		if (hmac == null)
			return false;
		else if (hmac.equals(getHash64(requestAuth + passphrase)))
			return true;
		else
			return false;
	}

	static File getFile(String name) {
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

	boolean queueRequest(String request) {
		boolean queued = false;
		if (sQueueFileName != null) {
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
			}
			if (queued)
				startQueueThread();
		}
		return queued;
	}

	String dequeueRequest() {
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

	void requeueRequest(String request) {
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

	void startQueueThread() {
		synchronized (sQueueLock) {
			// start the queue thread
			if (sQueueThread == null) {
				sQueueThread = new QueueThread(this, mQueueRetryInterval);
				sQueueThread.start();
			}
		}
	}

	boolean stopQueueThread() {
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