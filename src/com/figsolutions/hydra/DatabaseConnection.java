package com.figsolutions.hydra;

import org.json.simple.JSONObject;

public class DatabaseConnection {
	
	protected String mHostName;
	protected String mHostPort;
	protected String mAccountPath;
	protected String mUsername;
	protected String mPassword;
	protected String mDASU;
	protected String mDASP;
	protected String mSQLENVINIT;
	protected boolean mLock;
	
	public DatabaseConnection(String hostName, String hostPort, String accountPath, String username, String password) {
		mHostName = hostName;
		mHostPort = hostPort;
		mAccountPath = accountPath;
		mUsername = username;
		mPassword = password;
	}

	public DatabaseConnection(String hostName, String hostPort, String accountPath, String username, String password, String dasu, String dasp, String sqlenvinit) {
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
		HydraService.writeLog("get existing connection to: " + mAccountPath);
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
