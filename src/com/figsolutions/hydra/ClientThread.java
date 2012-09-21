package com.figsolutions.hydra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.sun.jndi.toolkit.url.Uri;

public class ClientThread extends Thread {

	protected static final String PARAM_AUTH = "auth";
	protected static final String PARAM_SALT = "salt";
	protected static final String PARAM_CHALLENGE = "challenge";
	protected static final String PARAM_DATABASE = "database";
	protected static final String PARAM_VALUES = "values";
	protected static final String PARAM_COLUMNS = "columns";
	protected static final String PARAM_SELECTION = "selection";
	protected static final String PARAM_STATEMENT = "statement";
	protected static final String PARAM_TARGET = "target";
	protected static final String PARAM_ACTION = "action";
	protected static final String ACTION_ABOUT = "about";
	protected static final String ACTION_QUERY = "query";
	protected static final String ACTION_INSERT = "insert";
	protected static final String ACTION_UPDATE = "update";
	protected static final String ACTION_EXECUTE = "execute";
	protected static final String ACTION_SUBROUTINE = "subroutine";
	protected static final String ACTION_DELETE = "delete";
	protected static final String BAD_REQUEST = "bad request";

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
		HydraRequest hydraRequest = null;
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
				if (line.startsWith("{"))
					hydraRequest = new HydraRequest((JSONObject) (new JSONParser()).parse(line));
				else
					hydraRequest = new HydraRequest(new Uri(line));
				if (hydraRequest.authenticated(challenge, saltedPassphrase)) {
					JSONObject response;
					// execute, select, update, insert, delete
					if (ACTION_ABOUT.equals(hydraRequest.action) && (hydraRequest.database.length() == 0))
						response = HydraService.getDatabases();
					else if (hydraRequest.database != null) {
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
								throw new Exception(BAD_REQUEST);

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
			if (BAD_REQUEST.equals(e.getMessage()) && (hydraRequest != null))
				HydraService.queueRequest(hydraRequest.getRequest());
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
}
