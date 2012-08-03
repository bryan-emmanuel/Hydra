<%@ page import="java.io.*"%>
<%@ page import="java.net.*"%>
<%@ page import="org.json.simple.JSONObject"%>
<%@ page import="java.security.MessageDigest"%>
<%@ page import="java.util.HashMap">

// not sure if these are needed
<%@ page import="org.xml.sax.InputSource"%>
<%@ page import="java.util.Map"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.net.URLDecoder"%>
<%@ page import="org.w3c.dom.*"%>
<%@ page import="javax.xml.parsers.*"%>

<%
// set response to value other than 200 for error
boolean debug_log = false;
String debug_log_file = "hydraclient.log";

PrintWriter logWriter;
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
		values += ","
	values += request.getParameter(s);
}

// if the request is valid, pass it to Hydra

try {
	String host = "1.1.1.1";
	String port = "9001";
	String passphrase = "figsolutions";
	
	Socket socket = new Socket(host, port);
	InputStream inStream = socket.getInputStream();
	OutputStream outStream = socket.getOutputStream();
	
	BufferedReader br = new BufferedReader(new InputStreamReader(in));
	String response = br.readLine();
	if ((response != null) && (response.length() > 0)) {
		
		JSONParser parser = new JSONParser();
		JSONObject obj = parser.parse(response);
		String salt = obj.get("salt");
		String challenge = obj.get("challenge");
		String request = "subroutine://debug/XFIG.PNP.BACKEND.CALLBACK?values=" + values + "&auth=";
		
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(salt + passphrase).getBytes("UTF-8");
		String saltedPassphrase = new BigInteger(1, md.digest()).toString(16);
		if (saltedPassphrase.length() > 64)
			saltedPassphrase = saltedPassphrase.substring(0, 64);
		
		md.reset();
		md.update(challenge + saltedPassphrase).getBytes("UTF-8"));
		String sessionAuth = new BigInteger(1, md.digest()).toString(16);
		if (sessionAuth.length() > 64)
			sessionAuth = sessionAuth.substring(0, 64);
		
		request += sessionAuth;
		request += "\n";
		// write the request
		outStream.write(request.getBytes);
		// read response back
		response = br.readLine();
		
	} catch (Exception e) {
		logWriter.println(e.printStackTrace);
	} catch (IOException e1) {
		logWriter.println(e1.printStackTrace);
	} catch (java.net.ConnectException e2) {
		logWriter.println(e2.printStackTrace);
	}
} else
	HttpServletResponse.sendError(400,"bad request"))
