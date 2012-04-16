package com.figsolutions.hydra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.sun.jndi.toolkit.url.Uri;

public class ClientThread implements Runnable {

	private int mClientIndex = 0;
	private Socket mSocket;

	public ClientThread(Socket socket, int clientIndex) {
		mSocket = socket;
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
			e.printStackTrace();
		}
		if ((out == null) || (in == null)) {
			return;
		}

		DatabaseConnection databaseConnection = null;
		// determine database type and get connection
		try {

			// requests take the form of:
			// <type>://<database>/<object>?properties=<p1>,<p2>,...

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			while ((line != null) && (line.length() > 0)) {
				System.out.println("read:"+line);
				Uri request = new Uri(line);
				String type = request.getScheme();
				String database = request.getHost();
				String object = request.getPath().substring(1);
				String rawQuery = request.getQuery().substring(1);
				System.out.println("type:"+type);
				System.out.println("database:"+database);
				System.out.println("object:"+object);
				System.out.println("rawQuery:"+rawQuery);
				String[] rawParams = rawQuery.split("&");
				HashMap<String, String> params = new HashMap<String, String>();
				for (String param : rawParams) {
					String[] pair = param.split("=");
					if (pair.length == 2) {
						String key = URLDecoder.decode(pair[0], "UTF-8");
						String value = URLDecoder.decode(pair[1], "UTF-8").replaceAll("(\\r|\\n)", "");
						System.out.println("param:"+key+"="+value);
						params.put(key, value);
					}
				}

				String response = "";
				// execute, select, update, insert, delete
				if (type.equals("about") && (database.length() == 0)) {
					response = HydraService.getDatabases();
				} else {

					databaseConnection = HydraService.getDatabaseConnection(database);

					if (type.equals("about")) {
						response = HydraService.getDatabase(database);
					} else if (type.equals("execute") && params.containsKey("statement")) {
						response = databaseConnection.execute(params.get("statement"));
					} else if (type.equals("query") && params.containsKey("columns")) {
						response = databaseConnection.query(object, params.get("columns").split(","), params.get("selection"));
					} else if (type.equals("update") && params.containsKey("columns") && params.containsKey("values") && params.containsKey("selection")) {
						response = databaseConnection.update(object, params.get("columns").split(","), params.get("values").split(","), params.get("selection"));
					} else if (type.equals("insert") && params.containsKey("columns") && params.containsKey("values")) {
						response = databaseConnection.insert(object, params.get("columns").split(","), params.get("values").split(","));
					} else if (type.equals("delete") && params.containsKey("selection")) {
						response = databaseConnection.delete(object, params.get("selection"));
					} else {
						throw new Exception("bad request");
					}

					// release the connection
					databaseConnection.disconnect();
				}

				System.out.println("response:"+response);
				out.write(response.getBytes());
				line = br.readLine();
			}
		} catch (Exception e) {
			JSONObject response = new JSONObject();
			JSONArray errors = new JSONArray();
			errors.add("Exception: " + e.getMessage());
			response.put("errors", errors);
			try {
				out.write(response.toString().getBytes());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			HydraService.removeClientThread(mClientIndex);
			if (databaseConnection != null) {
				try {
					databaseConnection.disconnect();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				mSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
