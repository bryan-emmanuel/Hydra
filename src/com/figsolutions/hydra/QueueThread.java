package com.figsolutions.hydra;

import static com.figsolutions.hydra.ClientThread.ACTION_DELETE;
import static com.figsolutions.hydra.ClientThread.ACTION_EXECUTE;
import static com.figsolutions.hydra.ClientThread.ACTION_INSERT;
import static com.figsolutions.hydra.ClientThread.ACTION_QUERY;
import static com.figsolutions.hydra.ClientThread.ACTION_SUBROUTINE;
import static com.figsolutions.hydra.ClientThread.ACTION_UPDATE;

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
