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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

public class AcceptThread extends Thread {

	private int mListenPort = 9001;
	public static final int DEFAULT_CONNECTIONS = -1;
	private int mConnections;
	private ArrayList<ClientThread> mClientThreads = new ArrayList<ClientThread>();
	private ServerSocket mServerSocket = null;

	public AcceptThread(int listenPort, int connections, String certFile, char[] certPass, char[] keystorePass) {
		mListenPort = listenPort;
		mConnections = connections;
		if ((certFile != null) && (certPass != null)) {
			HydraService.writeLog("create SSL socket");
			SSLContext sc = null;
			try {
				sc = SSLContext.getInstance("TLS");
			} catch (NoSuchAlgorithmException e) {
				HydraService.writeLog(e.getMessage());
			}
			if (sc != null) {
				KeyManagerFactory kmf = null;
				try {
					kmf = KeyManagerFactory.getInstance("SunX509");
				} catch (NoSuchAlgorithmException e) {
					HydraService.writeLog(e.getMessage());
				}
				if (kmf != null) {
					String certType;
					if (keystorePass != null)
						certType = "JKS";
					else {
						certType = "PKCS12";
						keystorePass = certPass;
					}
					HydraService.writeLog("certType: " + certType);
					FileInputStream fis = null;
					try {
						fis = new FileInputStream(certFile);
					} catch (FileNotFoundException e) {
						HydraService.writeLog(e.getMessage());
					}
					if (fis != null) {
						KeyStore ks = null;
						try {
							ks = KeyStore.getInstance(certType);
						} catch (KeyStoreException e) {
							HydraService.writeLog(e.getMessage());
						}
						if (ks != null) {
							SSLServerSocketFactory ssf = null; 
							try {
								ks.load(fis, keystorePass);
								kmf.init(ks, certPass);
								sc.init(kmf.getKeyManagers(), null, null);
								ssf = sc.getServerSocketFactory();
							} catch (NoSuchAlgorithmException e) {
								HydraService.writeLog(e.getMessage());
							} catch (CertificateException e) {
								HydraService.writeLog(e.getMessage());
							} catch (IOException e) {
								HydraService.writeLog(e.getMessage());
							} catch (UnrecoverableKeyException e) {
								HydraService.writeLog(e.getMessage());
							} catch (KeyStoreException e) {
								HydraService.writeLog(e.getMessage());
							} catch (KeyManagementException e) {
								HydraService.writeLog(e.getMessage());
							}
							try {
								fis.close();
							} catch (IOException e) {
								HydraService.writeLog(e.getMessage());
							}
							if (ssf != null) {
								try {
									mServerSocket = ssf.createServerSocket(listenPort);
								} catch (IOException e) {
									HydraService.writeLog(e.getMessage());
								}
							}
						}
					}
				}
			}
		} else {
			try {
				mServerSocket = new ServerSocket(mListenPort);
			} catch (IOException e) {
				HydraService.writeLog(e.getMessage());
			}
		}
	}

	public synchronized int removeClientThread(int index) {
		mClientThreads.remove(index);
		return mClientThreads.size();
	}

	@Override
	public void run() {
		while (mServerSocket != null) {
			try {
				HydraService.writeLog("listening...");
				Socket clientSocket = mServerSocket.accept();
				if ((mConnections == DEFAULT_CONNECTIONS) || (mClientThreads.size() < mConnections)) {
					ClientThread clientThread = new ClientThread(clientSocket, mClientThreads.size());
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
		HydraService.writeLog("AcceptThread shutdown");
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
