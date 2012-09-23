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

import org.json.simple.JSONObject;

public class DatabaseConnection {
	
	protected String mHostName;
	protected int mHostPort;
	protected String mAccountPath;
	protected String mUsername;
	protected String mPassword;
	protected String mDASU;
	protected String mDASP;
	protected String mSQLENVINIT;
	protected boolean mLock;
	protected HydraService mHydraService;
	
	public DatabaseConnection(HydraService hydraService, String hostName, int hostPort, String accountPath, String username, String password) {
		mHydraService = hydraService;
		mHostName = hostName;
		mHostPort = hostPort;
		mAccountPath = accountPath;
		mUsername = username;
		mPassword = password;
	}

	public DatabaseConnection(HydraService hydraService, String hostName, int hostPort, String accountPath, String username, String password, String dasu, String dasp, String sqlenvinit) {
		mHydraService = hydraService;
		mHostName = hostName;
		mHostPort = hostPort;
		mAccountPath = accountPath;
		mUsername = username;
		mPassword = password;
		mDASU = dasu;
		mDASP = dasp;
		mSQLENVINIT	= sqlenvinit;
	}
	
	public boolean connect() throws Exception {
		if (mLock)
			throw new Exception("connection locked");
		mHydraService.writeLog("get existing connection to: " + mAccountPath);
		mLock = true;
		return mLock;
	}
	
	public boolean isLocked() {
		return mLock;
	}
	
	public void release() {
		mLock = false;
	}
	
	public void disconnect() throws Exception {
	}
	
	public JSONObject execute(String statement) {
		return null;
	}
	
	public JSONObject query(String object, String[] columns, String selection) {
		return null;
	}
	
	public JSONObject insert(String object, String[] columns, String[] values) {
		return null;
	}
	
	public JSONObject update(String object, String[] columns, String[] values, String selection) {
		return null;
	}
	
	public JSONObject delete(String object, String selection) {
		return null;
	}
	
	public JSONObject subroutine(String object, String[] values) {
		return null;
	}
}
