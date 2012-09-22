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
