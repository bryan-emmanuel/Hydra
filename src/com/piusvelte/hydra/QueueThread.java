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

import static com.piusvelte.hydra.ClientThread.ACTION_DELETE;
import static com.piusvelte.hydra.ClientThread.ACTION_EXECUTE;
import static com.piusvelte.hydra.ClientThread.ACTION_INSERT;
import static com.piusvelte.hydra.ClientThread.ACTION_QUERY;
import static com.piusvelte.hydra.ClientThread.ACTION_SUBROUTINE;
import static com.piusvelte.hydra.ClientThread.ACTION_UPDATE;

import java.util.ArrayList;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class QueueThread extends Thread {

	public static final int DEFAULT_QUEUERETRYINTERVAL = 300000;
	protected int mQueueRetryInterval;
	private boolean mKeepAlive = true;

	public QueueThread(int queueRetryInterval) {
		mQueueRetryInterval = queueRetryInterval;
	}

	@Override
	public void run() {
		// sub-queue the requests
		while (mKeepAlive && HydraService.hasQueue()) {
			ArrayList<String> queue = HydraService.getQueue();
			while (mKeepAlive && !queue.isEmpty()) {
				String request = queue.remove(0);
				HydraService.writeLog("process queue: " + request);
				// process the request
				HydraRequest hydraRequest = null;
				try {
					hydraRequest = new HydraRequest((JSONObject) (new JSONParser()).parse(request));
				} catch (ParseException e) {
					hydraRequest = null;
					e.printStackTrace();
				}
				if ((hydraRequest != null) && (hydraRequest.database != null) && (!HydraService.getDatabase(hydraRequest.database).isEmpty())) {
					DatabaseConnection databaseConnection = null;
					try {
						databaseConnection = HydraService.getDatabaseConnection(hydraRequest.database);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (databaseConnection == null) {
						// still not available, re-queue
						if (hydraRequest.queueable)
							HydraService.queueRequest(hydraRequest.getRequest());
					} else {
						if (ACTION_EXECUTE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
							databaseConnection.execute(hydraRequest.target);
						else if (ACTION_QUERY.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0))
							databaseConnection.query(hydraRequest.target, hydraRequest.columns, hydraRequest.selection);
						else if (ACTION_UPDATE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
							databaseConnection.update(hydraRequest.target, hydraRequest.columns, hydraRequest.values, hydraRequest.selection);
						else if (ACTION_INSERT.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.columns.length > 0) && (hydraRequest.values.length > 0))
							databaseConnection.insert(hydraRequest.target, hydraRequest.columns, hydraRequest.values);
						else if (ACTION_DELETE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0))
							databaseConnection.delete(hydraRequest.target, hydraRequest.selection);
						else if (ACTION_SUBROUTINE.equals(hydraRequest.action) && (hydraRequest.target.length() > 0) && (hydraRequest.values.length > 0))
							databaseConnection.subroutine(hydraRequest.target, hydraRequest.values);
						// release the connection
						databaseConnection.release();
					}
				}
			}
			try {
				Thread.sleep(mQueueRetryInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		HydraService.stopQueueThread();
	}

	protected void shutdown() {
		mKeepAlive = false;
	}
}
