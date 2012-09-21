package com.figsolutions.hydra;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class AcceptThread extends Thread {
	
	private int mListenPort = 9001;
	public static final int DEFAULT_CONNECTIONS = -1;
	private int mConnections;
	private ArrayList<ClientThread> mClientThreads = new ArrayList<ClientThread>();
	private String mPassphrase;
	private String mSalt;
	private boolean mKeepAlive = true;

	public AcceptThread(int listenPort, int connections, String passphrase, String salt) {
		mListenPort = listenPort;
		mConnections = connections;
		mPassphrase = passphrase;
		mSalt = salt;
	}
	
	public synchronized int removeClientThread(int index) {
		if (index < mClientThreads.size()) {
			mClientThreads.get(index).shutdown();
			mClientThreads.remove(index);
		}
		return mClientThreads.size();
	}

	@Override
	public void run() {
		ServerSocket socket = null;
		
		// bind the port
		try {
			socket = new ServerSocket(mListenPort);
		} 
		catch (IOException e) {
			System.out.println("Could not listen on port: " + mListenPort);
			System.exit(-1);
		}
		
		if (socket != null) {
			// wait for connection requests
			while (mKeepAlive) {
				try {
					HydraService.writeLog("listening...");
					Socket client = socket.accept();
					if ((mConnections == DEFAULT_CONNECTIONS) || (mClientThreads.size() < mConnections)) {
						ClientThread clientThread = new ClientThread(client, mClientThreads.size(), mPassphrase, mSalt);
						mClientThreads.add(clientThread);
						clientThread.start();
						HydraService.writeLog("...start thread");
					}
				} catch (IOException e) {
					HydraService.writeLog(e.getMessage());
				}
			}
			// close down
			while (!mClientThreads.isEmpty())
				HydraService.removeClientThread(0);
			try {
				socket.close();
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
	}

	protected void shutdown() {
		mKeepAlive = false;
	}

}
