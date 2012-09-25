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
	private ServerSocket mServerSocket = null;

	public AcceptThread(int listenPort, int connections, String passphrase, String salt) {
		mListenPort = listenPort;
		mConnections = connections;
		mPassphrase = passphrase;
		mSalt = salt;
	}
	
	public synchronized int removeClientThread(int index) {
		mClientThreads.remove(index);
		return mClientThreads.size();
	}

	@Override
	public void run() {
		mServerSocket = null;

		// bind the port
		try {
			mServerSocket = new ServerSocket(mListenPort);
		} 
		catch (IOException e) {
			System.out.println("Could not listen on port: " + mListenPort);
			System.exit(-1);
		}

		// wait for connection requests
		while (mServerSocket != null) {
			try {
				HydraService.writeLog("listening...");
				Socket clientSocket = mServerSocket.accept();
				if ((mConnections == DEFAULT_CONNECTIONS) || (mClientThreads.size() < mConnections)) {
					ClientThread clientThread = new ClientThread(clientSocket, mClientThreads.size(), mPassphrase, mSalt);
					mClientThreads.add(clientThread);
					clientThread.start();
					HydraService.writeLog("...start thread");
				}
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
		while (!mClientThreads.isEmpty())
			mClientThreads.get(0).shutdown();
	}

	protected void shutdown() {
		if (mServerSocket != null) {
			try {
				mServerSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			mServerSocket = null;
		}
	}

}
