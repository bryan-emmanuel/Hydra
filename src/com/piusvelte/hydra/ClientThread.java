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

import static com.piusvelte.hydra.HydraRequest.PARAM_CHALLENGE;
import static com.piusvelte.hydra.HydraRequest.PARAM_SALT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sun.jndi.toolkit.url.Uri;

public class ClientThread extends Thread {

	static final String ACTION_ABOUT = "about";
	static final String ACTION_QUERY = "query";
	static final String ACTION_INSERT = "insert";
	static final String ACTION_UPDATE = "update";
	static final String ACTION_EXECUTE = "execute";
	static final String ACTION_SUBROUTINE = "subroutine";
	static final String ACTION_DELETE = "delete";
	static final String BAD_REQUEST = "bad request";

	// socket timeout
	private static final int sSocketTimeout = 60000;

	private int mClientIndex = 0;
	private Socket mSocket;
	private boolean mKeepAlive = true;

	public ClientThread(Socket socket, int clientIndex) {
		mSocket = socket;
		try {
			mSocket.setSoTimeout(sSocketTimeout);
		} catch (SocketException e) {
			HydraService.writeLog(e.getMessage());
		}
		mClientIndex = clientIndex;
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
		// send to the salt and challenge for authenticating requests
		JSONObject o = new JSONObject();
		o.put(PARAM_SALT, HydraService.getSalt());
		o.put(PARAM_CHALLENGE, Long.toString(challenge));
		try {
			out.write((o.toString() + "\n").getBytes());
		} catch (IOException e) {
			mKeepAlive = false;
			e.printStackTrace();
		}

		// requests take the form of:
		// <type>://<database>/<object>?properties=<p1>,<p2>,...

		// also supports JSON {type:,database:,object:,values:,columns:,values:}

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		while (mKeepAlive && (line != null) && (line.length() > 0)) {
			HydraService.writeLog("read: "+line);
			// get the request before auth for adding to the authentication
			if (line.startsWith("{")) {
				JSONObject request = null;
				try {
					request = (JSONObject) (new JSONParser().parse(line));
					hydraRequest = new HydraRequest(request);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			} else {
				Uri request = null;
				try {
					request = new Uri(line);
					hydraRequest = new HydraRequest(request);
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			}
			if ((hydraRequest != null) && HydraService.authenticated(Long.toString(challenge), hydraRequest.getHMAC(), hydraRequest.getRequestAuth())) {
				JSONObject response;
				// execute, select, update, insert, delete
				if (ACTION_ABOUT.equals(hydraRequest.action) && (hydraRequest.database.length() == 0))
					response = HydraService.getDatabases();
				else if (hydraRequest.database != null) {
					if (ACTION_ABOUT.equals(hydraRequest.action))
						response = HydraService.getDatabase(hydraRequest.database);
					else {
						try {
							databaseConnection = HydraService.getDatabaseConnection(hydraRequest.database);
						} catch (Exception e) {
							e.printStackTrace();
						}
						if (databaseConnection == null) {
							// queue
							response = new JSONObject();
							JSONArray errors = new JSONArray();
							errors.add("no database connection");
							if (hydraRequest.queueable) {
								HydraService.queueRequest(hydraRequest.getRequest());
								errors.add("queued");
							}
							response.put("errors", errors);
						} else {
							if (ACTION_EXECUTE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
								response = databaseConnection.execute(hydraRequest.target);
							else if (ACTION_QUERY.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0))
								response = databaseConnection.query(hydraRequest.target, hydraRequest.columns, hydraRequest.selection);
							else if (ACTION_UPDATE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
								response = databaseConnection.update(hydraRequest.target, hydraRequest.columns, hydraRequest.values, hydraRequest.selection);
							else if (ACTION_INSERT.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
								response = databaseConnection.insert(hydraRequest.target, hydraRequest.columns, hydraRequest.values);
							else if (ACTION_DELETE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
								response = databaseConnection.delete(hydraRequest.target, hydraRequest.selection);
							else if (ACTION_SUBROUTINE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.values.length > 0))
								response = databaseConnection.subroutine(hydraRequest.target, hydraRequest.values);
							else {
								response = new JSONObject();
								JSONArray errors = new JSONArray();
								errors.add(BAD_REQUEST);
								response.put("errors", errors);
							}
							// release the connection
							databaseConnection.release();
						}
					}
				} else {
					response = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add(BAD_REQUEST);
					response.put("errors", errors);
				}

				// update the challenge
				challenge = System.currentTimeMillis();
				response.put("challenge", Long.toString(challenge));

				HydraService.writeLog("response:"+response);
				try {
					out.write((response.toString() + "\n").getBytes());
				} catch (IOException e) {
					mKeepAlive = false;
					e.printStackTrace();
				}
			} else {
				JSONObject response = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(BAD_REQUEST);
				response.put("errors", errors);
				try {
					out.write(response.toString().getBytes());
				} catch (IOException e) {
					mKeepAlive = false;
					HydraService.writeLog(e.getMessage());
				}
			}
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// clean up everything
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
		if (mSocket != null) {
			try {
				mSocket.close();
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
		HydraService.clientThreadShutdown(mClientIndex);
	}

	protected void shutdown() {
		mKeepAlive = false;
	}
}
