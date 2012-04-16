package com.figsolutions.hydra;

public class DatabaseConnection {
	
	protected String mHostName;
	protected String mHostPort;
	protected String mAccountPath;
	protected String mUsername;
	protected String mPassword;
	protected long mLastConnected;
	protected long mTimeout;
	protected boolean mLock;
	
	public DatabaseConnection(String hostName, String hostPort, String accountPath, String username, String password, long timeout) {
		mHostName = hostName;
		mHostPort = hostPort;
		mAccountPath = accountPath;
		mUsername = username;
		mPassword = password;
		mTimeout = timeout;
	}
	
	public boolean connect() throws Exception {
		if (mLock) {
			throw new Exception("connection locked");
		}
		mLock = true;
		mLastConnected = System.currentTimeMillis();
		return mLock;
	}
	
	public void disconnect() throws Exception {
		mLock = false;
	}
	
	public boolean timeoutExpired() {
		return !mLock && System.currentTimeMillis() > (mLastConnected + mTimeout);
	}
	
	public String execute(String statement) {
		return null;
	}
	
	public String query(String object, String[] columns, String selection) {
		return null;
	}
	
	public String insert(String object, String[] columns, String[] values) {
		return null;
	}
	
	public String update(String object, String[] columns, String[] values, String selection) {
		return null;
	}
	
	public String delete(String object, String selection) {
		return null;
	}
}
