package com.figsolutions.hydra;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

public class AcceptThread implements Runnable {
	
	private int mListenPort = 9001;
	private int mClientThreadSize = 0;
	private ArrayList<Thread> mClientThreads = new ArrayList<Thread>();

	public AcceptThread(int listenPort, int clientThreadSize) {
		mListenPort = listenPort;
		mClientThreadSize = clientThreadSize;
	}
	
	public int getClientThreads() {
		return mClientThreads.size();
	}
	
	public void removeClientThread(int index) {
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
						Thread clientThread = new Thread(new ClientThread(client, mClientThreads.size()));
						mClientThreads.add(clientThread);						
						clientThread.start();
						clientThread.join();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
