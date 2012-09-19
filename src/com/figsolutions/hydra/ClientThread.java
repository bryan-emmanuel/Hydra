package com.figsolutions.hydra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.sun.jndi.toolkit.url.Uri;

public class ClientThread extends Thread {

	private static final String PARAM_AUTH = "auth";
	private static final String PARAM_SALT = "salt";
	private static final String PARAM_CHALLENGE = "challenge";
	private static final String PARAM_DATABASE = "database";
	private static final String PARAM_VALUES = "values";
	private static final String PARAM_COLUMNS = "columns";
	private static final String PARAM_SELECTION = "selection";
	private static final String PARAM_STATEMENT = "statement";
	private static final String PARAM_TARGET = "target";
	private static final String PARAM_ACTION = "action";
	private static final String ACTION_ABOUT = "about";
	private static final String ACTION_QUERY = "query";
	private static final String ACTION_INSERT = "insert";
	private static final String ACTION_UPDATE = "update";
	private static final String ACTION_EXECUTE = "execute";
	private static final String ACTION_SUBROUTINE = "subroutine";
	private static final String ACTION_DELETE = "delete";

	private int mClientIndex = 0;
	private Socket mSocket;
	private String mPassphrase;
	private String mSalt;
	private boolean mKeepAlive = true;

	public ClientThread(Socket socket, int clientIndex, String passphrase, String salt) {
		mSocket = socket;
		mClientIndex = clientIndex;
		mPassphrase = passphrase;
		mSalt = salt;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		OutputStream out = null;
		InputStream in = null;
		try {
			out = mSocket.getOutputStream();
			in = mSocket.getInputStream();
		} catch (IOException e) {
			HydraService.writeLog(e.getMessage());
		}
		if ((out == null) || (in == null))
			return;

		long challenge = System.currentTimeMillis();

		DatabaseConnection databaseConnection = null;
		// determine database type and get connection
		try {

			// salt the password
			String saltedPassphrase = HydraService.getHashString(mSalt + mPassphrase);
			if (saltedPassphrase.length() > 64)
				saltedPassphrase = saltedPassphrase.substring(0, 64);

			// send to the salt and challenge for authenticating requests
			JSONObject o = new JSONObject();
			o.put(PARAM_SALT, mSalt);
			o.put(PARAM_CHALLENGE, Long.toString(challenge));
			out.write((o.toString() + "\n").getBytes());

			// requests take the form of:
			// <type>://<database>/<object>?properties=<p1>,<p2>,...

			// also supports JSON {type:,database:,object:,values:,columns:,values:}

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			while (mKeepAlive && (line != null) && (line.length() > 0)) {
				HydraService.writeLog("read:"+line);
				// get the request before auth for adding to the authentication
				HydraRequest hydraRequest;
				if (line.startsWith("{"))
					hydraRequest = new HydraRequest((JSONObject) (new JSONParser()).parse(line));
				else
					hydraRequest = new HydraRequest(new Uri(line));
				if (hydraRequest.authenticated(challenge, saltedPassphrase)) {
					JSONObject response;
					// execute, select, update, insert, delete
					if (ACTION_ABOUT.equals(hydraRequest.action) && (hydraRequest.database.length() == 0)) {
						response = HydraService.getDatabases();
					} else if (hydraRequest.database != null) {
						if (ACTION_ABOUT.equals(hydraRequest.action))
							response = HydraService.getDatabase(hydraRequest.database);
						else {
							databaseConnection = HydraService.getDatabaseConnection(hydraRequest.database);
							if (ACTION_EXECUTE.equals(hydraRequest.action) && (hydraRequest.statement.length() > 0))
								response = databaseConnection.execute(hydraRequest.statement);
							else if (ACTION_QUERY.equals(hydraRequest.action) && (hydraRequest.columns.length > 0))
								response = databaseConnection.query(hydraRequest.target, hydraRequest.columns, hydraRequest.selection);
							else if (ACTION_UPDATE.equals(hydraRequest.action) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
								response = databaseConnection.update(hydraRequest.target, hydraRequest.columns, hydraRequest.values, hydraRequest.selection);
							else if (ACTION_INSERT.equals(hydraRequest.action) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
								response = databaseConnection.insert(hydraRequest.target, hydraRequest.columns, hydraRequest.values);
							else if (ACTION_DELETE.equals(hydraRequest.action))
								response = databaseConnection.delete(hydraRequest.target, hydraRequest.selection);
							else if (ACTION_SUBROUTINE.equals(hydraRequest.action) && (hydraRequest.values.length > 0))
								response = databaseConnection.subroutine(hydraRequest.target, hydraRequest.values);
							else
								throw new Exception("bad request");

							// release the connection
							databaseConnection.release();
						}
					} else
						response = new JSONObject();

					// update the challenge
					challenge = System.currentTimeMillis();
					response.put("challenge", Long.toString(challenge));

					HydraService.writeLog("response:"+response);
					out.write((response.toString() + "\n").getBytes());
					line = br.readLine();
				} else
					throw new Exception("authentication failed");
			}
		} catch (Exception e) {
			JSONObject response = new JSONObject();
			JSONArray errors = new JSONArray();
			errors.add("Exception: " + e.getMessage());
			response.put("errors", errors);
			try {
				out.write(response.toString().getBytes());
			} catch (IOException e1) {
				HydraService.writeLog(e1.getMessage());
			}
			e.printStackTrace();
		} finally {
			if (databaseConnection != null)
				databaseConnection.release();
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				mSocket.close();
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
			HydraService.removeClientThread(mClientIndex);
		}
	}
	
	protected void shutdown() {
		mKeepAlive = false;
	}

	class HydraRequest {

		String action = "";
		String database = "";
		String target = "";
		String[] columns = new String[0];
		String[] values = new String[0];
		String selection = "";
		String statement = "";
		String auth = "";
		String requestAuth = "";

		HydraRequest(JSONObject request) {
			action = (String) request.get(PARAM_ACTION);
			database = (String) request.get(PARAM_DATABASE);
			target = (String) request.get(PARAM_TARGET);
			columns = parseArray((JSONArray) request.get(PARAM_COLUMNS));
			values = parseArray((JSONArray) request.get(PARAM_VALUES));
			selection = (String) request.get(PARAM_SELECTION);
			statement = (String) request.get(PARAM_STATEMENT);
			auth = (String) request.get(PARAM_AUTH);
			requestAuth = action + database + target;
			for (String s : columns)
				requestAuth += s;
			for (String s : values)
				requestAuth += s;
			requestAuth += selection + statement;
		}

		HydraRequest(Uri request) {
			String rawQuery = null;
			action = request.getScheme();
			database = request.getHost();
			if (ACTION_ABOUT.equals(action) && database.startsWith("?")) {
				rawQuery = database.substring(1);
				database = "";
			} else {
				target = request.getPath();
				if ((target != null) && (target.length() > 1))
					target = target.substring(1);
				else
					target = "";
				rawQuery = request.getQuery();
				if ((rawQuery != null) && (rawQuery.length() > 1))
					rawQuery = rawQuery.substring(1);
			}
			// set the requestAuth
			requestAuth = request.toString();
			int authIdx = requestAuth.lastIndexOf("auth");
			requestAuth = requestAuth.substring(0, --authIdx);
			// parse parameters
			String[] rawParams = rawQuery.split("&");
			for (String param : rawParams) {
				String[] pair = param.split("=");
				if (pair.length == 2) {
					String key;
					String value;
					try {
						key = URLDecoder.decode(pair[0], "UTF-8");
						value = URLDecoder.decode(pair[1], "UTF-8").replaceAll("(\\r|\\n)", "");
						if (PARAM_COLUMNS.equals(key))
							columns = splitValues(value);
						else if (PARAM_VALUES.equals(key))
							values = splitValues(value);
						else if (PARAM_SELECTION.equals(key))
							selection = value;
						else if (PARAM_STATEMENT.equals(key))
							statement = value;
						else if (PARAM_AUTH.equals(key))
							auth = value;
						HydraService.writeLog("param:"+key+"="+value);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
			}
		}

		boolean authenticated(long challenge, String saltedPassphrase) {
			HydraService.writeLog("requestAuth: " + requestAuth);
			String passphrase;
			try {
				passphrase = HydraService.getHashString(requestAuth + Long.toString(challenge) + saltedPassphrase);
				if (passphrase.length() > 64)
					passphrase = passphrase.substring(0, 64);
				if (auth == null)
					return false;
				else
					return auth.equals(passphrase);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return false;
		}

		String[] parseArray(JSONArray jarr) {
			String[] sArr = new String[jarr.size()];
			for (int i = 0, l = sArr.length; i < l; i++)
				sArr[i] = (String) jarr.get(i);
			return sArr;
		}

		String[] splitValues(String valuesIn) {
			if (valuesIn.length() == 0)
				return new String[0];
			ArrayList<String> values = new ArrayList<String>();
			int nextIndex = -1;
			int fromIndex = 0;
			String value;
			while (((nextIndex = valuesIn.indexOf(",", fromIndex)) != -1) && (fromIndex < valuesIn.length())) {
				if (nextIndex == 0)
					value = "";
				else {
					// check if the comma is escaped
					if (valuesIn.substring((nextIndex - 1), nextIndex).equals("\\"))
						value = null;
					else {
						if (fromIndex == nextIndex)
							value = "";
						else {
							value = valuesIn.substring(fromIndex, nextIndex);
							if (value == null)
								value = "";
						}
					}
				}
				if (value != null)
					values.add(value);
				fromIndex = nextIndex + 1;
			}
			// add the remaining value
			if (fromIndex == valuesIn.length())
				value = "";
			else {
				value = valuesIn.substring(fromIndex);
				if (value  == null)
					value = "";
			}
			values.add(value);
			String[] valuesOut = new String[values.size()];
			for (int i = 0, l = valuesOut.length; i < l; i++)
				valuesOut[i] = values.get(i);
			return valuesOut;
		}

	}

}
