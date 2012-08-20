package com.figsolutions.hydra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.sun.jndi.toolkit.url.Uri;

public class ClientThread implements Runnable {

	private int mClientIndex = 0;
	private Socket mSocket;
	private String mPassphrase;
	private String mSalt;

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
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update((mSalt + mPassphrase).getBytes("UTF-8"));
			String saltedPassphrase = new BigInteger(1, md.digest()).toString(16);
			if (saltedPassphrase.length() > 64)
				saltedPassphrase = saltedPassphrase.substring(0, 64);

			// send to the salt and challenge for authenticating requests
			JSONObject o = new JSONObject();
			o.put("salt", mSalt);
			o.put("challenge", Long.toString(challenge));
			out.write((o.toString() + "\n").getBytes());

			// requests take the form of:
			// <type>://<database>/<object>?properties=<p1>,<p2>,...

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			while ((line != null) && (line.length() > 0)) {
				System.out.println("read:"+line);
				Uri request = new Uri(line);
				String type = request.getScheme();
				String database = request.getHost();
				String object = "";
				String rawQuery = "";
				if (type.equals("about") && database.startsWith("?")) {
					rawQuery = database.substring(1);
					database = "";
				} else {
					object = request.getPath();
					if ((object != null) && (object.length() > 0))
						object = object.substring(1);
					rawQuery = request.getQuery();
					if ((rawQuery != null) && (rawQuery.length() > 0))
						rawQuery = rawQuery.substring(1);
				}
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

				if (params.containsKey("auth")) {

					String auth = params.get("auth");

					// apply the challenge
					md.reset();
					md.update((Long.toString(challenge) + saltedPassphrase).getBytes("UTF-8"));
					String passphrase = new BigInteger(1, md.digest()).toString(16);
					if (passphrase.length() > 64)
						passphrase = passphrase.substring(0, 64);

					if (auth.equals(passphrase)) {

						JSONObject response;
						// execute, select, update, insert, delete
						if (type.equals("about") && (database.length() == 0)) {
							response = HydraService.getDatabases();
						} else {

							if (type.equals("about")) {
								response = HydraService.getDatabase(database);
							} else {
								databaseConnection = HydraService.getDatabaseConnection(database);
								if (type.equals("execute") && params.containsKey("statement")) {
									response = databaseConnection.execute(params.get("statement"));
								} else if (type.equals("query") && params.containsKey("columns")) {
									response = databaseConnection.query(object, params.get("columns").split(","), params.get("selection"));
								} else if (type.equals("update") && params.containsKey("columns") && params.containsKey("values") && params.containsKey("selection")) {
									response = databaseConnection.update(object, params.get("columns").split(","), params.get("values").split(","), params.get("selection"));
								} else if (type.equals("insert") && params.containsKey("columns") && params.containsKey("values")) {
									response = databaseConnection.insert(object, params.get("columns").split(","), params.get("values").split(","));
								} else if (type.equals("delete") && params.containsKey("selection")) {
									response = databaseConnection.delete(object, params.get("selection"));
								} else if (type.equals("subroutine") && params.containsKey("arguments") && params.containsKey("values")) {
									response = databaseConnection.subroutine(object, params.get("values").split(","));
								} else {
									throw new Exception("bad request");
								}

								// release the connection
								databaseConnection.release();
							}
						}

						// update the challenge
						challenge = System.currentTimeMillis();
						response.put("challenge", Long.toString(challenge));

						System.out.println("response:"+response);
						out.write((response.toString() + "\n").getBytes());
						line = br.readLine();
					} else
						throw new Exception("authentication failed");
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
			HydraService.removeClientThread(mClientIndex);
			if (databaseConnection != null)
				databaseConnection.release();
			try {
				mSocket.close();
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
	}

}
