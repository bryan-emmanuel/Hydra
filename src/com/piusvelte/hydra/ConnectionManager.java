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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletContext;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ConnectionManager {

	private static final String sPassphrase = "passphrase";
	private static final String sHydra = "hydra";
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
	private String passphrase = null;
	private int[] databaseLock = new int[0];
	private HashMap<String, HashMap<String, String>> sDatabaseSettings = new HashMap<String, HashMap<String, String>>();
	private HashMap<String, ArrayList<DatabaseConnection>> sDatabaseConnections = new HashMap<String, ArrayList<DatabaseConnection>>();
	private HashMap<String, Integer> queuedDatabaseRequests = new HashMap<String, Integer>();
	private String sHydraDir = null;
	private String sQueueFile = null;
	private String tokenFile = null;
	private int[] sQueueLock = new int[0];
	private String sHydraProperties = "/WEB-INF/hydra.properties";
	private QueueThread sQueueThread = null;
	private int mQueueRetryInterval;
	private int[] tokenLock = new int[0];
	private ArrayList<String> tokens = new ArrayList<String>();
	private HashMap<String, String> unauthorizedTokens = new HashMap<String, String>();

	private static ConnectionManager hydraService = null;

	private ConnectionManager(ServletContext ctx) {

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
			if (properties.containsKey(sHydra)) {
				sHydraDir = properties.getProperty(sHydra);
				if (sHydraDir.length() > 0) {
					if (!File.separator.equals(sHydraDir.substring(sHydraDir.length() - 1, sHydraDir.length())))
						sHydraDir += File.separator;
					sQueueFile = sHydraDir + "queue";
					tokenFile = sHydraDir + "tokens";
					try {
						loadTokens();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
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

	public static ConnectionManager getService(ServletContext ctx) {
		if (hydraService == null) {
			hydraService = new ConnectionManager(ctx);
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
		JSONArray arr = new JSONArray();
		synchronized (databaseLock) {
			Set<String> databases = sDatabaseSettings.keySet();
			Iterator<String> iter = databases.iterator();
			while (iter.hasNext())
				arr.add(iter.next());
		}
		response.put("result", arr);
		return response;
	}

	@SuppressWarnings("unchecked")
	JSONObject getDatabase(String database) {
		JSONObject response = new JSONObject();
		JSONObject props = new JSONObject();
		synchronized (databaseLock) {
			if (sDatabaseSettings.containsKey(database)) {
				HashMap<String, String> databaseSettings = sDatabaseSettings.get(database);
				props.put(sAlias, database);
				props.put(sDatabase, databaseSettings.get(sDatabase));
				props.put(sHost, databaseSettings.get(sHost));
				props.put(sPort, databaseSettings.get(sPort));
				props.put(sType, databaseSettings.get(sType));
			}
		}
		response.put("result", props);
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

	public boolean isAuthenticated(String token) {
		if (token == null)
			return false;
		else
			return tokens.contains(token);
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

	void loadTokens() throws Exception {
		if (tokenFile != null) {
			synchronized (tokenLock) {
				File f = getFile(tokenFile);
				if (f != null) {
					FileReader fr = null;
					try {
						fr = new FileReader(f);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
						throw new Exception("error loading tokens file");
					}
					BufferedReader br = new BufferedReader(fr);
					String r = null;
					try {
						r = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					while (r != null) {
						tokens.add(getHash64(r + passphrase));
						try {
							r = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
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

	String createToken() throws Exception {
		if (tokenFile != null) {
			String token = new BigInteger(256, new SecureRandom()).toString(16);
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("SHA-256");
				md.update(token.getBytes("UTF-8"));
				token = new BigInteger(1, md.digest()).toString(16);
				if (token.length() > 64)
					token = token.substring(0, 64);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				throw new Exception("error generating token");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				throw new Exception("error generating token");
			}
			synchronized (tokenLock) {
				unauthorizedTokens.put(getHash64(token + passphrase), token);
				return token;
			}
		} else
			throw new Exception("no tokens available");
	}

	void authorizeToken(String token) throws Exception {
		if (tokenFile != null) {
			synchronized (tokenLock) {
				if (unauthorizedTokens.containsKey(token)) {
					File f = getFile(tokenFile);
					if (f != null) {
						FileWriter fw = null;
						try {
							fw = new FileWriter(f, true);
						} catch (IOException e) {
							e.printStackTrace();
							throw new Exception("error storing token");
						}
						PrintWriter pw = new PrintWriter(fw);
						pw.println(unauthorizedTokens.get(token));
						pw.close();
						try {
							fw.close();
						} catch (IOException e) {
							e.printStackTrace();
							throw new Exception("error storing token");
						}
						unauthorizedTokens.remove(token);
					}
				}
			}
		} else
			throw new Exception("no tokens available");
	}

	boolean queueRequest(String request) {
		boolean queued = false;
		if (sQueueFile != null) {
			synchronized (sQueueLock) {
				File queueFile = getFile(sQueueFile);
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
			File queueFile = getFile(sQueueFile);
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
			File queueFile = getFile(sQueueFile);
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
		if (sQueueFile != null) {
			synchronized (sQueueLock) {
				if (sQueueThread == null) {
					sQueueThread = new QueueThread(this, mQueueRetryInterval);
					sQueueThread.start();
				}
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