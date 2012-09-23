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

import java.util.Scanner;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

public class HydraLauncher implements Daemon {
	
	// daemon
	private static HydraService hydraService = null;
	private static HydraLauncher hydraServiceInstance = new HydraLauncher();

	public static void main(String[] args) {
		hydraServiceInstance.initialize();

		Scanner sc = new Scanner(System.in);
		// wait until receive stop command from keyboard
		System.out.printf("Enter 'stop' to halt: ");
		while(!sc.nextLine().toLowerCase().equals("stop"));

		if (!hydraService.isShutdown())
			hydraServiceInstance.terminate();
	}
	
	/**
	 * Static methods called by prunsrv to start/stop
	 * the Windows service.  Pass the argument "start"
	 * to start the service, and pass "stop" to
	 * stop the service.
	 *
	 * Taken lock, stock and barrel from Christopher Pierce's blog at http://blog.platinumsolutions.com/node/234
	 *
	 * @param args Arguments from prunsrv command line
	 **/
	public static void windowsService(String args[]) {
		String cmd = "start";
		if (args.length > 0)
			cmd = args[0];

		if ("start".equals(cmd))
			hydraServiceInstance.windowsStart();
		else
			hydraServiceInstance.windowsStop();
	}

	public void windowsStart() {
		initialize();
		while (!hydraService.isShutdown()) {
			synchronized(this) {
				try {
					this.wait(60000);  // wait 1 minute and check if stopped
				}
				catch(InterruptedException ie){}
			}
		}
	}

	public void windowsStop() {
		terminate();
		synchronized(this) {
			this.notify();
		}
	}

	// Implementing the Daemon interface is not required for Windows but is for Linux
	@Override
	public void init(DaemonContext arg0) throws Exception {
	}

	@Override
	public void start() {
		initialize();
	}

	@Override
	public void stop() {
		terminate();
	}

	@Override
	public void destroy() {
	}
	
	private void initialize() {
		if (hydraService == null)
			(hydraService = new HydraService()).start();
	}

	public void terminate() {
		if (hydraService != null)
			hydraService.shutdown();
	}
}