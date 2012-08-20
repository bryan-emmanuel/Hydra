<%@ page import="java.io.*"%>
<%@ page import="java.net.*"%>
<%@ page import="java.security.MessageDigest"%>
<%@ page import="java.math.BigInteger"%>
//<%@ page import="java.util.HashMap"%>

// not sure if these are needed
/*
<%@ page import="org.xml.sax.InputSource"%>
<%@ page import="java.util.Map"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.net.URLDecoder"%>
<%@ page import="org.w3c.dom.*"%>
<%@ page import="javax.xml.parsers.*"%>
*/

<%
// set response to value other than 200 for error
boolean debug_log = true;
String debug_log_file = "hydraclient.log";

PrintWriter logWriter = null;
if (debug_log) {
	logWriter = new PrintWriter(new FileOutputStream(debug_log_file));
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
 * Order is important as they will be passed this way to the subroutine
 * - TransNum ?
 * - StatusCD
 * - Amount ?total amount?
 * - Convenience Fee ?
 * - OrderID
 * - PmtDeviceLast4
 * - PmtDeviceType
 */
String[] pnpParams = new String[]{"TransNum","StatusCD","Amount","ConvenienceFee","OrderID","PmtDeviceLast4","PmtDeviceType"};
String values = "";
for (String s : pnpParams) {
	if (values != "")
		values += ",";
	values += request.getParameter(s);
}
String hydraResponse = "";
Boolean isValid = false;
// if the request is valid, pass it to Hydra

try {
	String host = "usci-data1.jungle.usip.edu";
	int port = 9001;
	String passphrase = "figsolutions";
	
	Socket socket = new Socket(host, port);
	InputStream inStream = socket.getInputStream();
	OutputStream outStream = socket.getOutputStream();
	
	BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
	hydraResponse = br.readLine();
	String salt = null;
	String challenge = null;
	if ((hydraResponse != null) && (hydraResponse.length() > ("{salt:\"\",challenge:\"\"").length())) {
		// the response is json, but we know what's there, so just skip a library
		// {salt:"",challenge:""}
		if (hydraResponse.substring(0,7) == "{salt:\"") {
			hydraResponse = hydraResponse.substring(7);
			int comma = hydraResponse.indexOf(",");
			if (comma > 0) {
				salt = hydraResponse.substring(0,comma);
				if (hydraResponse.substring(0,12) == ",challenge:\"") {
					hydraResponse = hydraResponse.substring(12);
					int end = hydraResponse.indexOf("\"");
					if (end > 0)
						challenge = hydraResponse.substring(0,end);
				}
			}
		}
	}
	if ((salt != null) && (challenge != null)) {
		String hydraRequest = "subroutine://debug/XFIG.PNP.BACKEND.CALLBACK?values=" + values + "&auth=";
	
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update((salt + passphrase).getBytes("UTF-8"));
		String saltedPassphrase = new BigInteger(1, md.digest()).toString(16);
		if (saltedPassphrase.length() > 64)
			saltedPassphrase = saltedPassphrase.substring(0, 64);
	
		md.reset();
		md.update((challenge + saltedPassphrase).getBytes("UTF-8"));
		String sessionAuth = new BigInteger(1, md.digest()).toString(16);
		if (sessionAuth.length() > 64)
			sessionAuth = sessionAuth.substring(0, 64);
	
		hydraRequest += sessionAuth;
		hydraRequest += "\n";
		// write the request
		outStream.write(hydraRequest.getBytes());
		// read response back
		hydraResponse = br.readLine();
	} else
		isValid = false;

} catch (Exception e) {
	isValid = false;
	if (logWriter != null)
		e.printStackTrace(logWriter);
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
%>