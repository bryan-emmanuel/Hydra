package com.figsolutions.hydra;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class AcceptThread implements Runnable {
	
	private int mListenPort = 9001;
	private int mClientThreadSize = 1;
	private ArrayList<Thread> mClientThreads = new ArrayList<Thread>();
	private String mPassphrase;
	private String mSalt;

	public AcceptThread(int listenPort, int clientThreadSize, String passphrase, String salt) {
		mListenPort = listenPort;
		mClientThreadSize = clientThreadSize;
		mPassphrase = passphrase;
		mSalt = salt;
	}
	
	public synchronized int getClientThreads() {
		return mClientThreads.size();
	}
	
	public synchronized void removeClientThread(int index) {
		mClientThreads.remove(index);
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
					Socket client = socket.accept();
					if (mClientThreads.size() < mClientThreadSize) {
						Thread clientThread = new Thread(new ClientThread(client, mClientThreads.size(), mPassphrase, mSalt));
						mClientThreads.add(clientThread);						
						clientThread.start();
						clientThread.join();
					}
				} catch (IOException e) {
					HydraService.writeLog(e.getMessage());
				} catch (InterruptedException e) {
					HydraService.writeLog(e.getMessage());
				}
			}
			
			try {
				socket.close();
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
	}

}
