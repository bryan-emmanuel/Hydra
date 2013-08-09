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

import static com.piusvelte.hydra.ApiServlet.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.Scanner;

import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HydraRequest {
	
	String action = null;
	String database = null;
	String target = null;
	String[] columns = null;
	String[] values = null;
	String[] arguments = null;
	String selection = null;
	boolean queueable = false;
	String command = null;
	
	public HydraRequest(String queuedRequest) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject request = (JSONObject) parser.parse(queuedRequest);
		if (request.containsKey(PARAM_ACTION))
			action = (String) request.get(PARAM_ACTION);
		if (request.containsKey(PARAM_DATABASE))
			database = (String) request.get(PARAM_DATABASE);
		if (request.containsKey(PARAM_TARGET))
			target = (String) request.get(PARAM_TARGET);
		if (request.containsKey(PARAM_ACTION))
			action = (String) request.get(PARAM_ACTION);
		if (request.containsKey(PARAM_COLUMNS))
			columns = parseArray(parser, (String) request.get(PARAM_COLUMNS));
		else
			columns = new String[0];
		if (request.containsKey(PARAM_VALUES))
			values = parseArray(parser, (String) request.get(PARAM_VALUES));
		else
			values = new String[0];
		if (request.containsKey(PARAM_SELECTION))
			selection = (String) request.get(PARAM_SELECTION);
		if (request.containsKey(PARAM_QUEUEABLE))
			queueable = (Boolean) request.get(PARAM_QUEUEABLE);
		if (request.containsKey(PARAM_COMMAND))
			command = (String) request.get(PARAM_COMMAND);
	}
	
	private HydraRequest(HttpServletRequest request) throws UnsupportedEncodingException {
		String[] parts = getPathParts(request);
		database = parts[DATABASE];
		target = parts[TARGET];
		columns = unpackArray(request.getParameter(PARAM_COLUMNS));
		values = unpackArray(request.getParameter(PARAM_VALUES));
		arguments = unpackArgumentsEntity(request);
		selection = request.getParameter(PARAM_SELECTION);
		if (selection != null) {
			selection = URLDecoder.decode(selection, "UTF-8");
		}
		String q = request.getParameter(PARAM_QUEUEABLE);
		if (q != null) {
			queueable = Boolean.parseBoolean(q);
		}
		command = request.getParameter(PARAM_COMMAND);
		if (command != null) {
			command = URLDecoder.decode(command, "UTF-8");
		}
	}
	
	private String[] unpackArgumentsEntity(HttpServletRequest request) {
		try {
			StringBuffer responseAsStringBuffer = new StringBuffer();
			Scanner scanner = new Scanner(request.getInputStream());
			
			while(scanner.hasNextLine())
				responseAsStringBuffer.append(scanner.nextLine());
			
			return unpackArray(responseAsStringBuffer.toString());
			
		} catch (IOException e) {
			System.err.println("HYDRA " + new Date() + "There was a problem getting the arguments entity. Code: AE01");
			e.printStackTrace();
			
			return null;
		}
	}
	
	private String[] unpackArray(String packedArray) {
		if (packedArray == null) {
			return new String[0];
		} else {
			String[] arr = packedArray.split(",", -1);
			// decode nested commas
			for (int i = 0; i < arr.length; i++) {
				try {
					arr[i] = URLDecoder.decode(arr[i], "UTF-8");
				} catch (UnsupportedEncodingException e) {
				}
			}
			return arr;
		}
	}
	
	public static HydraRequest fromGet(HttpServletRequest request) throws Exception {
		HydraRequest hydraRequest;
		try {
			hydraRequest = new HydraRequest(request);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new Exception("bad parameter encoding");
		}
		hydraRequest.action = ACTION_QUERY;
		return hydraRequest;
	}
	
	public static HydraRequest fromPost(HttpServletRequest request) throws Exception {
		HydraRequest hydraRequest;
		try {
			hydraRequest = new HydraRequest(request);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new Exception("bad parameter encoding");
		}
		if (!hydraRequest.hasDatabase())
			throw new Exception("no database");
		if (hydraRequest.hasColumns())
			hydraRequest.action = ACTION_INSERT;
		else if (hydraRequest.hasArguments())
			hydraRequest.action = ACTION_SUBROUTINE;
		else if (hydraRequest.hasCommand())
			hydraRequest.action = ACTION_EXECUTE;
		else
			throw new Exception("invalid request");
		return hydraRequest;
	}
	
	public static HydraRequest fromPut(HttpServletRequest request) throws Exception {
		HydraRequest hydraRequest;
		try {
			hydraRequest = new HydraRequest(request);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new Exception("bad parameter encoding");
		}
		if (!hydraRequest.hasDatabase() || !hydraRequest.hasTarget())
			throw new Exception("invalid request");
		hydraRequest.action = ACTION_UPDATE;
		return hydraRequest;
	}
	
	public static HydraRequest fromDelete(HttpServletRequest request) throws Exception {
		HydraRequest hydraRequest;
		try {
			hydraRequest = new HydraRequest(request);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new Exception("bad parameter encoding");
		}
		if (!hydraRequest.hasDatabase() || !hydraRequest.hasTarget())
			throw new Exception("invalid request");
		hydraRequest.action = ACTION_DELETE;
		return hydraRequest;
	}
	
	private String[] getPathParts(HttpServletRequest request) {
		String path = request.getRequestURI().substring(request.getContextPath().length() + 4);
		String[] parts = new String[]{null, null};
		if (path.length() > 0) {
			if (path.substring(0, 1).equals("/"))
				path = path.substring(1);
			if (path.length() > 0) {
				String[] paths = path.split("/", -1);
				if (paths[DATABASE].length() > 0)
					parts[DATABASE] = paths[DATABASE];
				if ((paths.length > TARGET) && (paths[TARGET].length() > 0))
					parts[TARGET] = paths[TARGET];
			}
		}
		return parts;
	}
	
	public boolean hasDatabase() {
		return database != null;
	}
	
	public boolean hasTarget() {
		return target != null;
	}
	
	private boolean hasColumns() {
		return columns.length > 0;
	}
	
	private boolean hasArguments() {
		return arguments.length > 0;
	}
	
	private boolean hasCommand() {
		return command != null;
	}
	
	public boolean isInsert() {
		return ACTION_INSERT.equals(action);
	}
	
	public boolean isSubroutine() {
		return ACTION_SUBROUTINE.equals(action);
	}
	
	public boolean isExecute() {
		return ACTION_EXECUTE.equals(action);
	}
	
	private String[] parseArray(JSONParser parser, String obj) {
		try {
			JSONArray jsonArr = (JSONArray) parser.parse(obj);
			int s = jsonArr.size();
			String[] arr = new String[s];
			for (int i = 0; i < s; i++)
				arr[s] = (String) jsonArr.get(s);
			return arr;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new String[0];
	}
	
	private JSONArray packArray(String[] arr) {
		JSONArray jsonArr = new JSONArray();
		for (String s : arr)
			jsonArr.add(s);
		return jsonArr;
	}

	public String toJSONString() {
		JSONObject request = new JSONObject();
		request.put(PARAM_ACTION, action);
		request.put(PARAM_DATABASE, database);
		request.put(PARAM_TARGET, target);
		request.put(PARAM_COLUMNS, packArray(columns));
		request.put(PARAM_VALUES, packArray(values));
		request.put(PARAM_ARGUMENTS, packArray(arguments));
		request.put(PARAM_SELECTION, selection);
		request.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		request.put(PARAM_COMMAND, command);
		return request.toJSONString();
	}
}
