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

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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

	/*
	 * query
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if (request.getParameter(PARAM_TOKEN) == null)
			response.getWriter().write("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Hydra</title></head><body><h3>Hydra API</h3>");
		else {
			ServletContext ctx = getServletContext();
			HydraRequest hydraRequest;
			try {
				hydraRequest = HydraRequest.fromGet(request);
			} catch (Exception e) {
				ctx.log(e.getMessage());
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				response.setStatus(402);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
				return;
			}
			ConnectionManager connMgr = ConnectionManager.getInstance(ctx);
			if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
				if (hydraRequest.hasDatabase()) {
					if (hydraRequest.hasTarget()) {
						DatabaseConnection databaseConnection = null;
						connMgr.queueDatabaseRequest(hydraRequest.database);
						try {
							while (databaseConnection == null)
								databaseConnection = connMgr.getDatabaseConnection(hydraRequest.database);
						} catch (Exception e) {
							ctx.log("Hydra: " + e.getMessage());
						}
						connMgr.dequeueDatabaseRequest(hydraRequest.database);
						if (databaseConnection != null) {
							response.getWriter().write(databaseConnection.query(hydraRequest.target, hydraRequest.columns, hydraRequest.selection).toJSONString());
							databaseConnection.release();
						} else if (hydraRequest.queueable) {
							JSONObject j = new JSONObject();
							JSONArray errors = new JSONArray();
							errors.add("no database connection");
							connMgr.queueRequest(hydraRequest.toJSONString());
							errors.add("queued");
							response.setStatus(200);
							j.put("errors", errors);
							response.getWriter().write(j.toJSONString());
						} else {
							response.setStatus(502);
						}
						connMgr.cleanDatabaseConnections(hydraRequest.database);
					} else {
						response.getWriter().write(connMgr.getDatabase(hydraRequest.database).toJSONString());
					}
				} else {
					response.getWriter().write(connMgr.getDatabases().toJSONString());
				}
			} else {
				ctx.log("not authenticated");
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
		ConnectionManager connMgr = ConnectionManager.getInstance(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			HydraRequest hydraRequest;
			try {
				hydraRequest = HydraRequest.fromPost(request);
			} catch (Exception e) {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				response.setStatus(402);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
				return;
			}
			if (hydraRequest.database != null) {
				DatabaseConnection databaseConnection = null;
				connMgr.queueDatabaseRequest(hydraRequest.database);
				try {
					while (databaseConnection == null)
						databaseConnection = connMgr.getDatabaseConnection(hydraRequest.database);
				} catch (Exception e) {
					e.printStackTrace();
				}
				connMgr.dequeueDatabaseRequest(hydraRequest.database);
				if (databaseConnection != null) {
					if (hydraRequest.isInsert())
						response.getWriter().write(databaseConnection.insert(hydraRequest.target, hydraRequest.columns, hydraRequest.values).toJSONString());
					else if (hydraRequest.isSubroutine())
						response.getWriter().write(databaseConnection.subroutine(hydraRequest.target, hydraRequest.arguments).toJSONString());
					else if (hydraRequest.isExecute())
						response.getWriter().write(databaseConnection.execute(hydraRequest.command).toJSONString());
					else {
						JSONObject j = new JSONObject();
						JSONArray errors = new JSONArray();
						errors.add("invalid request");
						response.setStatus(403);
						j.put("errors", errors);
						response.getWriter().write(j.toJSONString());
					}
					databaseConnection.release();
				} else if (hydraRequest.queueable) {
					JSONObject j = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add("no database connection");
					connMgr.queueRequest(hydraRequest.toJSONString());
					errors.add("queued");
					response.setStatus(200);
					j.put("errors", errors);
					response.getWriter().write(j.toJSONString());
				} else {
					response.setStatus(502);
					JSONObject j = new JSONObject();
					JSONArray errors = new JSONArray();
					errors.add("no database connection available");
					j.put("errors", errors);
					response.getWriter().write(j.toJSONString());
				}
				connMgr.cleanDatabaseConnections(hydraRequest.database);
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
		ConnectionManager connMgr = ConnectionManager.getInstance(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			HydraRequest hydraRequest;
			try {
				hydraRequest = HydraRequest.fromPut(request);
			} catch (Exception e) {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				response.setStatus(402);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
				return;
			}
			DatabaseConnection databaseConnection = null;
			connMgr.queueDatabaseRequest(hydraRequest.database);
			try {
				while (databaseConnection == null)
					databaseConnection = connMgr.getDatabaseConnection(hydraRequest.database);
			} catch (Exception e) {
				e.printStackTrace();
				
			}
			connMgr.dequeueDatabaseRequest(hydraRequest.database);
			if (databaseConnection != null) {
				response.getWriter().write(databaseConnection.update(hydraRequest.target, hydraRequest.columns, hydraRequest.values, hydraRequest.selection).toJSONString());
				databaseConnection.release();
			} else if (hydraRequest.queueable) {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add("no database connection");
				connMgr.queueRequest(hydraRequest.toJSONString());
				errors.add("queued");
				response.setStatus(200);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
			} else {
				response.setStatus(502);
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add("no database connection available");
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
			}
			connMgr.cleanDatabaseConnections(hydraRequest.database);
		} else {
			response.setStatus(401);
		}
	}

	/*
	 * delete
	 */
	public void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ConnectionManager connMgr = ConnectionManager.getInstance(getServletContext());
		if (connMgr.isAuthenticated(request.getParameter(PARAM_TOKEN))) {
			HydraRequest hydraRequest;
			try {
				hydraRequest = HydraRequest.fromPut(request);
			} catch (Exception e) {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				response.setStatus(402);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
				return;
			}
			DatabaseConnection databaseConnection = null;
			connMgr.queueDatabaseRequest(hydraRequest.database);
			try {
				while (databaseConnection == null)
					databaseConnection = connMgr.getDatabaseConnection(hydraRequest.database);
			} catch (Exception e) {
				e.printStackTrace();
			}
			connMgr.dequeueDatabaseRequest(hydraRequest.database);
			if (databaseConnection != null) {
				response.getWriter().write(databaseConnection.delete(hydraRequest.target, hydraRequest.selection).toJSONString());
				databaseConnection.release();
			} else if (hydraRequest.queueable) {
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add("no database connection");
				connMgr.queueRequest(hydraRequest.toJSONString());
				errors.add("queued");
				response.setStatus(200);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
			} else {
				response.setStatus(502);
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add("no database connection available");
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
			}
			connMgr.cleanDatabaseConnections(hydraRequest.database);
		} else {
			response.setStatus(401);
		}
	}

}
