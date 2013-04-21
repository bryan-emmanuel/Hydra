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

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@WebServlet("/api/*")
public class ApiServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	static final int DATABASE = 0;
	static final int TARGET = 1;

	static final String PARAM_TOKEN = "token";
	static final String PARAM_COLUMNS = "columns";
	static final String PARAM_SELECTION = "selection";
	static final String PARAM_VALUES = "values";
	static final String PARAM_ARGUMENTS = "arguments";
	static final String PARAM_COMMAND = "command";
	static final String PARAM_QUEUEABLE = "queueable";
	static final String PARAM_TARGET = "target";
	static final String PARAM_ACTION = "action";
	static final String PARAM_DATABASE = "database";

	static final String ACTION_QUERY = "query";
	static final String ACTION_INSERT = "insert";
	static final String ACTION_UPDATE = "update";
	static final String ACTION_EXECUTE = "execute";
	static final String ACTION_SUBROUTINE = "subroutine";
	static final String ACTION_DELETE = "delete";

	/*
	 * METHODS:
	 *  GET - query
	 *  POST - insert, subroutine, execute
	 *  PUT - update
	 *  DELETE - delete
	 */

	private String[] getPathParts(HttpServletRequest request) {
		String path = request.getRequestURI().substring(request.getContextPath().length() + 4);
		String[] parts = new String[]{null, null};
		if (path.length() > 0) {
			if (path.substring(0, 1).equals("/"))
				path = path.substring(1);
			if (path.length() > 0) {
				String[] paths = path.split("/");
				if (paths[DATABASE].length() > 0)
					parts[DATABASE] = paths[DATABASE];
				if ((paths.length > TARGET) && (paths[TARGET].length() > 0))
					parts[TARGET] = paths[TARGET];
			}
		}
		return parts;
	}

	private String getQueuedRequest(String action, String database, String target, String[] columns, String[] values, String selection, boolean queueable) {
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

	/*
	 * query
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String[] parts = getPathParts(request);
		String database = parts[DATABASE];
		String target = parts[TARGET];
		String[] columns = request.getParameterValues(PARAM_COLUMNS);
		String[] values = request.getParameterValues(PARAM_VALUES);
		String selection = request.getParameter(PARAM_SELECTION);
		String q = request.getParameter(PARAM_QUEUEABLE);
		boolean queueable = false;
		if (q == null)
			q = "";
		else
			queueable = Boolean.parseBoolean(q);
		if (request.getParameter(PARAM_TOKEN) == null)
			response.getWriter().write("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Hydra</title></head><body><h3>Hydra API</h3>");
		else {
			ConnectionManager connMgr = ConnectionManager.getService(getServletContext());
			if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
				if (database != null) {
					if (target != null) {
						DatabaseConnection databaseConnection = null;
						connMgr.queueDatabaseRequest(database);
						try {
							while (databaseConnection == null)
								databaseConnection = connMgr.getDatabaseConnection(database);
						} catch (Exception e) {
							e.printStackTrace();
						}
						connMgr.dequeueDatabaseRequest(database);
						if (databaseConnection != null) {
							response.getWriter().write(databaseConnection.query(target, columns, selection).toJSONString());
							databaseConnection.release();
						} else if (queueable) {
							JSONObject j = new JSONObject();
							JSONArray errors = new JSONArray();
							errors.add("no database connection");
							connMgr.queueRequest(getQueuedRequest(ACTION_QUERY, database, target, columns, values, selection, queueable));
							errors.add("queued");
							response.setStatus(200);
							j.put("errors", errors);
							response.getWriter().write(j.toJSONString());
						} else
							response.setStatus(502);
						connMgr.cleanDatabaseConnections(database);
					} else
						response.getWriter().write(connMgr.getDatabase(database).toJSONString());
				} else
					response.getWriter().write(connMgr.getDatabases().toJSONString());
			} else {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add("not authenticated");
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
				response.setStatus(401);
			}
		}
	}

	/*
	 * insert
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String[] parts = getPathParts(request);
		String database = parts[DATABASE];
		String target = parts[TARGET];
		String[] columns = request.getParameterValues(PARAM_COLUMNS);
		String[] values = request.getParameterValues(PARAM_VALUES);
		String selection = request.getParameter(PARAM_SELECTION);
		String[] arguments = request.getParameterValues(PARAM_ARGUMENTS);
		String command = request.getParameter(PARAM_ARGUMENTS);
		String q = request.getParameter(PARAM_QUEUEABLE);
		boolean queueable = false;
		if (q == null)
			q = "";
		else
			queueable = Boolean.parseBoolean(q);
		ConnectionManager connMgr = ConnectionManager.getService(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			if (database != null) {
				String action = null;
				DatabaseConnection databaseConnection = null;
				connMgr.queueDatabaseRequest(database);
				try {
					while (databaseConnection == null)
						databaseConnection = connMgr.getDatabaseConnection(database);
				} catch (Exception e) {
					e.printStackTrace();
				}
				connMgr.dequeueDatabaseRequest(database);
				if (databaseConnection != null) {
					if (columns.length > 0) {
						action = ACTION_INSERT;
						response.getWriter().write(databaseConnection.insert(target, columns, values).toJSONString());
					} else if (arguments.length > 0) {
						action = ACTION_SUBROUTINE;
						response.getWriter().write(databaseConnection.subroutine(target, arguments).toJSONString());
					} else if (command != null) {
						action = ACTION_EXECUTE;
						response.getWriter().write(databaseConnection.execute(command).toJSONString());
					} else {
						JSONObject j = new JSONObject();
						JSONArray errors = new JSONArray();
						errors.add("invalid request");
						response.setStatus(403);
						j.put("errors", errors);
						response.getWriter().write(j.toJSONString());
					}
					databaseConnection.release();
				} else if (queueable) {
					JSONObject j = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add("no database connection");
					connMgr.queueRequest(getQueuedRequest(action, database, target, columns, values, selection, queueable));
					errors.add("queued");
					response.setStatus(200);
					j.put("errors", errors);
					response.getWriter().write(j.toJSONString());
				} else
					response.setStatus(502);
				connMgr.cleanDatabaseConnections(database);
			} else
				response.setStatus(402);
		} else {
			response.setStatus(401);
		}
	}

	/*
	 * update
	 */
	public void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String[] parts = getPathParts(request);
		String database = parts[DATABASE];
		String target = parts[TARGET];
		String[] columns = request.getParameterValues(PARAM_COLUMNS);
		String[] values = request.getParameterValues(PARAM_VALUES);
		String selection = request.getParameter(PARAM_SELECTION);
		String q = request.getParameter(PARAM_QUEUEABLE);
		boolean queueable = false;
		if (q == null)
			q = "";
		else
			queueable = Boolean.parseBoolean(q);
		ConnectionManager connMgr = ConnectionManager.getService(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			if ((database != null) && (target != null)) {
				DatabaseConnection databaseConnection = null;
				connMgr.queueDatabaseRequest(database);
				try {
					while (databaseConnection == null)
						databaseConnection = connMgr.getDatabaseConnection(database);
				} catch (Exception e) {
					e.printStackTrace();
				}
				connMgr.dequeueDatabaseRequest(database);
				if (databaseConnection != null) {
					response.getWriter().write(databaseConnection.query(target, columns, selection).toJSONString());
					databaseConnection.release();
				} else if (queueable) {
					JSONObject j = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add("no database connection");
					connMgr.queueRequest(getQueuedRequest(ACTION_UPDATE, database, target, columns, values, selection, queueable));
					errors.add("queued");
					response.setStatus(200);
					j.put("errors", errors);
					response.getWriter().write(j.toJSONString());
				} else
					response.setStatus(502);
				connMgr.cleanDatabaseConnections(database);
			} else
				response.setStatus(402);
		} else {
			response.setStatus(401);
		}
	}

	/*
	 * delete
	 */
	public void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String[] parts = getPathParts(request);
		String database = parts[DATABASE];
		String target = parts[TARGET];
		String[] columns = request.getParameterValues(PARAM_COLUMNS);
		String[] values = request.getParameterValues(PARAM_VALUES);
		String selection = request.getParameter(PARAM_SELECTION);
		String q = request.getParameter(PARAM_QUEUEABLE);
		boolean queueable = false;
		if (q == null)
			q = "";
		else
			queueable = Boolean.parseBoolean(q);
		ConnectionManager connMgr = ConnectionManager.getService(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			if ((database != null) && (target != null)) {
				DatabaseConnection databaseConnection = null;
				connMgr.queueDatabaseRequest(database);
				try {
					while (databaseConnection == null)
						databaseConnection = connMgr.getDatabaseConnection(database);
				} catch (Exception e) {
					e.printStackTrace();
				}
				connMgr.dequeueDatabaseRequest(database);
				if (databaseConnection != null) {
					response.getWriter().write(databaseConnection.query(target, columns, selection).toJSONString());
					databaseConnection.release();
				} else if (queueable) {
					JSONObject j = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add("no database connection");
					connMgr.queueRequest(getQueuedRequest(ACTION_DELETE, database, target, columns, values, selection, queueable));
					errors.add("queued");
					response.setStatus(200);
					j.put("errors", errors);
					response.getWriter().write(j.toJSONString());
				} else
					response.setStatus(502);
				connMgr.cleanDatabaseConnections(database);
			} else
				response.setStatus(402);
		} else {
			response.setStatus(401);
		}
	}

}
