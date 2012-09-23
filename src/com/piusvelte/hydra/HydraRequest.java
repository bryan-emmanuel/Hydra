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

import static com.piusvelte.hydra.ClientThread.ACTION_ABOUT;
import static com.piusvelte.hydra.ClientThread.PARAM_ACTION;
import static com.piusvelte.hydra.ClientThread.PARAM_AUTH;
import static com.piusvelte.hydra.ClientThread.PARAM_COLUMNS;
import static com.piusvelte.hydra.ClientThread.PARAM_DATABASE;
import static com.piusvelte.hydra.ClientThread.PARAM_QUEUEABLE;
import static com.piusvelte.hydra.ClientThread.PARAM_SELECTION;
import static com.piusvelte.hydra.ClientThread.PARAM_TARGET;
import static com.piusvelte.hydra.ClientThread.PARAM_VALUES;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.sun.jndi.toolkit.url.Uri;

public class HydraRequest {

	String action = "";
	String database = "";
	String target = "";
	String[] columns = new String[0];
	String[] values = new String[0];
	String selection = "";
	String auth = "";
	String requestAuth = "";
	boolean queueable = false;

	HydraRequest(JSONObject request) {
		action = (String) request.get(PARAM_ACTION);
		if (action == null)
			action = "";
		database = (String) request.get(PARAM_DATABASE);
		if (database == null)
			database = "";
		target = (String) request.get(PARAM_TARGET);
		if (target == null)
			target = "";
		columns = parseArray((JSONArray) request.get(PARAM_COLUMNS));
		values = parseArray((JSONArray) request.get(PARAM_VALUES));
		selection = (String) request.get(PARAM_SELECTION);
		if (selection == null)
			selection = "";
		String q = (String) request.get(PARAM_QUEUEABLE);
		if (q == null)
			q = "";
		else
			queueable = Boolean.parseBoolean(q);
		auth = (String) request.get(PARAM_AUTH);
		requestAuth = action + database + target;
		for (String s : columns)
			requestAuth += s;
		for (String s : values)
			requestAuth += s;
		requestAuth += selection + q;
	}
	
	@SuppressWarnings("unchecked")
	String getRequest() {
		JSONObject request = new JSONObject();
		request.put(PARAM_ACTION, action);
		request.put(PARAM_DATABASE, database);
		request.put(PARAM_TARGET, target);
		JSONArray colArr = new JSONArray();
		for (String s : columns)
			colArr.add(s);
		request.put(PARAM_COLUMNS, colArr);
		JSONArray valArr = new JSONArray();
		for (String s : values)
			valArr.add(s);
		request.put(PARAM_VALUES, valArr);
		request.put(PARAM_SELECTION, selection);
		request.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return request.toJSONString();
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
					else if (PARAM_QUEUEABLE.equals(key))
						queueable = Boolean.parseBoolean(value);
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