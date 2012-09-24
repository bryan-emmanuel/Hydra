<%@ page import="java.io.*"%>
<%@ page import="java.net.*"%>
<%@ page import="java.security.MessageDigest"%>
<%@ page import="java.math.BigInteger"%>
<%@ page import="java.util.*"%>

// <%@ page import="org.xml.sax.InputSource"%>
// <%@ page import="java.util.Map"%>
<%@ page import="java.util.Date"%>
// <%@ page import="java.net.URLDecoder"%>
// <%@ page import="org.w3c.dom.*"%>
// <%@ page import="javax.xml.parsers.*"%>

<%
// set response to value other than 200 for error
boolean debug_log = true;
String debug_log_file = "fig.pnp.postback.log";

PrintWriter logWriter = null;
if (debug_log) {
	logWriter = new PrintWriter(new BufferedWriter(new FileWriter(debug_log_file, true)));
	// check that the file was opened
	if (logWriter != null)
		logWriter.println("# start request : " + (new Date()).toString());
	else
		debug_log = false;
}

// first parse the request from PNP and validate it

String key_postbackXML = "postbackXML";
String postbackURL = null;
String postbackParams = null;

// map the PNP params to the original params expected by the callback
//String[] pnpParams = new String[]{"ClientSessionID","TransNum","InvoiceNo","PONum","StatusCD","OrderID","PmtDeviceLast4","PmtDeviceType","Amount","TotalFeeAmount","TotalAmountPaid"};

/*
if (logWriter != null) {
	Enumeration parameters = request.getParameterNames();
	while (parameters.hasMoreElements()) {
		String name = parameters.nextElement().toString();
		logWriter.println(name + "=" + request.getParameter(name));
	}
}
*/

/*
 * Order is important as they will be passed this way to the subroutine
 * - TransNum ? use InvoiceNO instead
 * - StatusCD
 * - Amount ?total amount?
 * - Convenience Fee ?
 * - OrderID
 * - PmtDeviceLast4
 * - PmtDeviceType
 * - Result
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
String hydraResponse = "";
Boolean isValid = false;
// if the request is valid, pass it to Hydra

if (values.length() > 0) {
	String hydraRequest = "subroutine://debug/XFIG.PNP.POSTBACK?values=" + values;
	// write the request
	if (logWriter != null)
		logWriter.println("request: " + hydraRequest);
	Socket socket = null;
	InputStream inStream = null;
	OutputStream outStream = null;
	try {
		String host = "webaddebug.usciences.edu";
		int port = 9001;
		String passphrase = "figsolutions";
		
		if (logWriter != null)
			logWriter.println("open socket");
		socket = new Socket(host, port);
		inStream = socket.getInputStream();
		outStream = socket.getOutputStream();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		hydraResponse = br.readLine();
		if (logWriter != null)
			logWriter.println("challenge response: " + hydraResponse);
		String salt = null;
		String challenge = null;
		if ((hydraResponse != null) && (hydraResponse.length() > 22)) {
			// the response is json, but we know what's there, so just skip a library
			// {salt:"",challenge:""}
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
			outStream.write(hydraRequest.getBytes());
			// read response back
			hydraResponse = br.readLine();
			if (logWriter != null)
				logWriter.println("response: " + hydraResponse);
			// check the result, ex: {"result":["value1","value2","value3","value4","value5"]}
			if (hydraResponse == null)
				isValid = false;
			else {
				// get the array bounds
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
	
	} catch (Exception e) {
		isValid = false;
		if (logWriter != null)
			e.printStackTrace(logWriter);
	} finally {
		if (socket != null) {
			if (inStream != null) {
				try {
					inStream.close();
				} catch (IOException e) {
					if (logWriter != null)
						logWriter.println(e.getMessage());
				}
			}
			if (outStream != null) {
				try {
					outStream.close();
				} catch (IOException e) {
					if (logWriter != null)
						logWriter.println(e.getMessage());
				}
			}
			if (logWriter != null)
				logWriter.println("close socket");
			try {
				socket.close();
			} catch (IOException e) {
				if (logWriter != null)
					logWriter.println(e.getMessage());
			}
		}
	}
} else if (logWriter != null) {
	logWriter.println("no parameters");
}
if (isValid) {
%>
<h1>OK</h1>
<%
} else {
		response.sendError(400,"bad request");
%>
<h1>Error</h1>
<p><%=hydraResponse%></p>
<%
}
if (logWriter != null)
	logWriter.close();
%>