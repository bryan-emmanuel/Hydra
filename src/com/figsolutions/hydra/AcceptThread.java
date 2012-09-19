package com.figsolutions.hydra;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class AcceptThread implements Runnable {
	
	private int mListenPort = 9001;
	private int mClientThreadSize = 1;
	private ArrayList<ClientThread> mClientThreads = new ArrayList<ClientThread>();
	private String mPassphrase;
	private String mSalt;

	public AcceptThread(int listenPort, int clientThreadSize, String passphrase, String salt) {
		mListenPort = listenPort;
		mClientThreadSize = clientThreadSize;
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
		boolean listening = true;
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
			while (listening) {
				try {
					HydraService.writeLog("listening...");
					Socket client = socket.accept();
					if (mClientThreads.size() < mClientThreadSize) {
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

}
