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

public class AuthServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		ServletContext servletContext = getServletContext();
		ConnectionManager connMgr = ConnectionManager.getInstance(servletContext);
		JSONObject j = new JSONObject();
		String token = request.getParameter("token");
		if ((token != null) && (token.length() > 0)) {
			try {
				connMgr.authorizeToken(token);
			} catch (Exception e) {
				servletContext.log(e.getMessage());
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				j.put("errors", errors);
				response.setStatus(403);
			}
		} else {
			try {
				j.put("result", connMgr.createToken());
			} catch (Exception e) {
				servletContext.log(e.getMessage());
				JSONArray errors = new JSONArray();
				errors.add(e.getMessage());
				j.put("errors", errors);
				response.setStatus(403);
			}
		}
		response.getWriter().write(j.toJSONString());
	}

}
