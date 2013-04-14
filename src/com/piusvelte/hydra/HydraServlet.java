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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class HydraServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	static final String ACTION_ABOUT = "about";
	static final String ACTION_QUERY = "query";
	static final String ACTION_INSERT = "insert";
	static final String ACTION_UPDATE = "update";
	static final String ACTION_EXECUTE = "execute";
	static final String ACTION_SUBROUTINE = "subroutine";
	static final String ACTION_DELETE = "delete";
	static final String BAD_REQUEST = "bad request";

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.getWriter().write("use POST");
		response.setStatus(401);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HydraRequest hydraRequest = new HydraRequest(request);
		if ((hydraRequest != null) && HydraService.authenticated(null, hydraRequest.getHMAC(), hydraRequest.getRequestAuth())) {
			DatabaseConnection databaseConnection = null;
			// execute, select, update, insert, delete
			if (ACTION_ABOUT.equals(hydraRequest.action) && (hydraRequest.database.length() == 0)) {
				response.setStatus(200);
				response.getWriter().write(HydraService.getDatabases().toJSONString());
			} else if (hydraRequest.database != null) {
				if (ACTION_ABOUT.equals(hydraRequest.action)) {
					response.setStatus(200);
					response.getWriter().write(HydraService.getDatabase(hydraRequest.database).toJSONString());
				} else {
					try {
						databaseConnection = HydraService.getDatabaseConnection(hydraRequest.database);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (databaseConnection == null) {
						// queue
						JSONObject j = new JSONObject();
						JSONArray errors = new JSONArray();
						errors.add("no database connection");
						if (hydraRequest.queueable) {
							HydraService.queueRequest(hydraRequest.getRequest());
							errors.add("queued");
							response.setStatus(200);
						} else
							response.setStatus(400);
						j.put("errors", errors);
						response.getWriter().write(j.toJSONString());
					} else {
						response.setStatus(200);
						if (ACTION_EXECUTE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
							response.getWriter().write(databaseConnection.execute(hydraRequest.target).toJSONString());
						else if (ACTION_QUERY.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0))
							response.getWriter().write(databaseConnection.query(hydraRequest.target, hydraRequest.columns, hydraRequest.selection).toJSONString());
						else if (ACTION_UPDATE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
							response.getWriter().write(databaseConnection.update(hydraRequest.target, hydraRequest.columns, hydraRequest.values, hydraRequest.selection).toJSONString());
						else if (ACTION_INSERT.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
							response.getWriter().write(databaseConnection.insert(hydraRequest.target, hydraRequest.columns, hydraRequest.values).toJSONString());
						else if (ACTION_DELETE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
							response.getWriter().write(databaseConnection.delete(hydraRequest.target, hydraRequest.selection).toJSONString());
						else if (ACTION_SUBROUTINE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.values.length > 0))
							response.getWriter().write(databaseConnection.subroutine(hydraRequest.target, hydraRequest.values).toJSONString());
						else {
							response.setStatus(400);
							JSONObject j = new JSONObject();
							JSONArray errors = new JSONArray();
							errors.add(BAD_REQUEST);
							j.put("errors", errors);
							response.getWriter().write(j.toJSONString());
						}
						// release the connection
						databaseConnection.release();
					}
				}
			} else {
				response.setStatus(400);
				JSONObject j = new JSONObject();
				JSONArray errors = new JSONArray();
				errors.add(BAD_REQUEST);
				j.put("errors", errors);
				response.getWriter().write(j.toJSONString());
			}
		} else {
			response.setStatus(400);
			JSONObject j = new JSONObject();
			JSONArray errors = new JSONArray();
			errors.add(BAD_REQUEST);
			j.put("errors", errors);
			response.getWriter().write(j.toJSONString());
		}
	}

}
