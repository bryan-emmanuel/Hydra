<%@ page import="java.io.*"%>
<%@ page import="java.net.*"%>
<%@ page import="java.security.MessageDigest"%>
<%@ page import="java.math.BigInteger"%>
<%@ page import="java.util.*"%>
<%@ page import="java.util.Date"%>
<%@ page import="javax.net.SocketFactory"%>
<%@ page import="javax.net.ssl.SSLSocketFactory"%>
<%
/*
 * Configure connection parameters for Hydra here
 */
String host = "";
int port = 9001;
String passphrase = "";
boolean use_ssl = true;
String database = "";
boolean debug_log = true;
/*
 * End Configuration 
 */

/*
 * Open the log file if appropriate
 */
String debug_log_file = "fig.pnp.postback.log";
PrintWriter logWriter = null;
if (debug_log) {
	logWriter = new PrintWriter(new BufferedWriter(new FileWriter(debug_log_file, true)));
	if (logWriter != null)
		logWriter.println("# start request : " + (new Date()).toString());
	else
		debug_log = false;
}

/*
 * Parse the parameters in the PNP request
 */
String[] pnpParams = new String[]{"InvoiceNo","StatusCD","Amount","ConvenienceFee","OrderID","PmtDeviceLast4","PmtDeviceType","Result"};
String values = "";
for (String s : pnpParams) {
	if (values != "")
		values += ",";
	String p = request.getParameter(s);
	if (p != null)
		values += p;
}
/*
 * Pass the parameters to Colleauge via Hydra
 */
String hydraResponse = "";
Boolean isValid = false;
if (values.length() > 0) {
	String hydraRequest = "subroutine://" + database + "/XFIG.PNP.POSTBACK?values=" + values;
	if (debug_log)
		logWriter.println("request: " + hydraRequest);
	Socket socket = null;
	InputStream inStream = null;
	OutputStream outStream = null;
	try {
		// Connect to Hydra
		if (use_ssl) {
			SocketFactory sf = SSLSocketFactory.getDefault();
			socket = sf.createSocket(host, port);
		} else
			socket = new Socket(host, port);
		inStream = socket.getInputStream();
		outStream = socket.getOutputStream();		
		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		hydraResponse = br.readLine();
		String salt = null;
		String challenge = null;
		if ((hydraResponse != null) && (hydraResponse.length() > 22)) {
			// parse the returned salt and challenge for assembling the HMAC
			if (hydraResponse.substring(0,14).equals("{\"challenge\":\"")) {
				hydraResponse = hydraResponse.substring(14);
				int comma = hydraResponse.indexOf(",");
				if (comma > 0) {
					challenge = hydraResponse.substring(0,(comma - 1));
					hydraResponse = hydraResponse.substring(comma);
					if (hydraResponse.substring(0,9).equals(",\"salt\":\"")) {
						hydraResponse = hydraResponse.substring(9);
						int end = hydraResponse.indexOf("\"");
						if (end > 0)
							salt = hydraResponse.substring(0,end);
					}
				}
			} else if (hydraResponse.substring(0,9).equals("{\"salt\":\"")) {
				hydraResponse = hydraResponse.substring(9);
				int comma = hydraResponse.indexOf(",");
				if (comma > 0) {
					salt = hydraResponse.substring(0,(comma - 1));
					hydraResponse = hydraResponse.substring(comma);
					if (hydraResponse.substring(0,14).equals(",\"challenge\":\"")) {
						hydraResponse = hydraResponse.substring(14);
						int end = hydraResponse.indexOf("\"");
						if (end > 0)
							challenge = hydraResponse.substring(0,end);
					}
				}
			}
		}
		if ((salt != null) && (challenge != null)) {
			// salt the passphrase
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update((salt + passphrase).getBytes("UTF-8"));
			StringBuffer hexString = new StringBuffer();
			byte[] hash = md.digest();
			for (byte b : hash) {
				if ((0xFF & b) < 0x10)
					hexString.append("0" + Integer.toHexString((0xFF & b)));
				else
					hexString.append(Integer.toHexString(0xFF & b));
			}
			String saltedPassphrase = hexString.toString();
			if (saltedPassphrase.length() > 64)
				saltedPassphrase = saltedPassphrase.substring(0, 64);
			md.reset();
			// build the final HMAC from the request, challenge and saltedPassphrase
			md.update((hydraRequest + challenge + saltedPassphrase).getBytes("UTF-8"));
			hexString = new StringBuffer();
			hash = md.digest();
			for (byte b : hash) {
				if ((0xFF & b) < 0x10)
					hexString.append("0" + Integer.toHexString((0xFF & b)));
				else
					hexString.append(Integer.toHexString(0xFF & b));
			}
			String sessionAuth = hexString.toString();
			if (sessionAuth.length() > 64)
				sessionAuth = sessionAuth.substring(0, 64);
			hydraRequest += "&hmac=" + sessionAuth;
			hydraRequest += "\n";
			// send the request to Hydra
			outStream.write(hydraRequest.getBytes());
			// read the response
			hydraResponse = br.readLine();
			if (debug_log)
				logWriter.println("response: " + hydraResponse);
			if (hydraResponse == null)
				isValid = false;
			else {
				// the last argument returned is either: 0, or 1, for Success, or Error, respectively
				int arrIdx = hydraResponse.indexOf("\"]");
				if (arrIdx < 1)
					isValid = false;
				else {
					hydraResponse = hydraResponse.substring((arrIdx - 1), arrIdx);
					if (hydraResponse.equals("0"))
						isValid = true;
					else
						isValid = false;
				}
			}
		} else
			isValid = false;
		if (debug_log)
			logWriter.println("isValid: " + isValid);
	} catch (Exception e) {
		isValid = false;
		if (debug_log)
			e.printStackTrace(logWriter);
	} finally {
		if (socket != null) {
			if (inStream != null) {
				try {
					inStream.close();
				} catch (IOException e) {
					if (debug_log)
						e.printStackTrace(logWriter);
				}
			}
			if (outStream != null) {
				try {
					outStream.close();
				} catch (IOException e) {
					if (logWriter != null)
						e.printStackTrace(logWriter);
				}
			}
			try {
				socket.close();
			} catch (IOException e) {
				if (debug_log)
					e.printStackTrace(logWriter);
			}
		}
	}
} else if (debug_log)
	logWriter.println("no parameters");
if (debug_log)
	logWriter.close();
/*
 * A successful return from Colleague should be a normal 200 response, otherwise a 400
 */
if (isValid) {
%>
<h1>OK</h1>
<%
} else {
		response.sendError(400, "bad request");
%>
<h1>Error</h1>
<p><%=hydraResponse%></p>
<%
}
%>
