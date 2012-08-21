package com.figsolutions.hydra;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

//import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniCommand;
import asjava.uniobjects.UniCommandException;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniObjectsTokens;
import asjava.uniobjects.UniSelectList;
import asjava.uniobjects.UniSelectListException;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;
import asjava.uniobjects.UniSubroutine;
import asjava.uniobjects.UniSubroutineException;

public class UnidataConnection extends DatabaseConnection {

	UniSession mSession;
	private static final String SIMPLE_QUERY_FORMAT = "SELECT %s";
	private static final String SELECTION_QUERY_FORMAT = "SELECT %s WITH %s";

	public UnidataConnection(String hostName, String hostPort, String accountPath, String username, String password, String dasu, String dasp, String sqlenvinit) {
		super(hostName, hostPort, accountPath, username, password, dasu, dasp, sqlenvinit);
	}

	@Override
	public boolean connect() throws Exception {
		super.connect();
		if (mSession == null) {
			mSession = new UniSession();
			mSession.setHostName(mHostName);
			mSession.setHostPort(Integer.parseInt(mHostPort));
			mSession.setAccountPath(mAccountPath);
			mSession.setUserName(mUsername);
			mSession.setPassword(mPassword);
			mSession.setConnectionString("udcs");
			mSession.connect();
			// need to initialize MIO
			String hydraInitMio = "HYDRA.INIT.MIO";
			UniFile uFile = mSession.open("BP");
			String code = "X.DASU = SETENV('DASU','" + mDASU + "')";
			code += UniObjectsTokens.FM_CHAR + "X.DASP = SETENV('DASP','" + mDASP + "')";
			uFile.write(hydraInitMio, code);
			UniCommand uCommand = mSession.command("BASIC BP " + hydraInitMio);
			uCommand.exec();
			uCommand.setCommand("CATALOG BP " + hydraInitMio + " DIRECT FORCE");
			uCommand.exec();
			uCommand.setCommand(hydraInitMio);
			uCommand.exec();
			uCommand.setCommand("SQLENVINIT DMI:" + mSQLENVINIT);
			uCommand.exec();
		}
		return mLock;
	}

	@Override
	public void disconnect() throws Exception {
		super.disconnect();
		if (mSession != null)
			mSession.disconnect();
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject execute(String statement) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		try {
			UniCommand uCommand = mSession.command();
			uCommand.setCommand(statement);
			uCommand.exec();
			response.put("result", uCommand.response());
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		}
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject query(String object, String[] columns, String selection) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		UniFile uFile = null;

		try {
			JSONArray result = new JSONArray();
			UniCommand uCommand = mSession.command();
			if (selection == null) {
				uCommand.setCommand(String.format(SIMPLE_QUERY_FORMAT, object).toString());
			} else {
				uCommand.setCommand(String.format(SELECTION_QUERY_FORMAT, object, selection).toString());
			}
			UniSelectList uSelect = mSession.selectList(0);
			uCommand.exec();
			uFile = mSession.openFile(object);
			UniString recordID = null;
			while ((recordID = uSelect.next()).length() > 0) {
				uFile.setRecordID(recordID);
				for (String column : columns) {
					JSONObject col = new JSONObject();
					col.put(column, uFile.readNamedField(column).toString());
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniFileException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniSelectListException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} finally {
			if (uFile != null) {
				try {
					uFile.close();
				} catch (UniFileException e) {
					errors.add(e.getMessage());
					HydraService.writeLog(e.getMessage());
				}
			}
		}
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject insert(String object, String[] columns, String[] values) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject update(String object, String[] columns, String[] values, String selection) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		UniFile uFile = null;

		try {
			JSONArray result = new JSONArray();
			UniCommand uCommand = mSession.command();
			if (selection == null)
				uCommand.setCommand(String.format(SIMPLE_QUERY_FORMAT, object).toString());
			else
				uCommand.setCommand(String.format(SELECTION_QUERY_FORMAT, object, selection).toString());
			UniSelectList uSelect = mSession.selectList(0);
			uCommand.exec();
			uFile = mSession.openFile(object);
			UniString recordID = null;
			while ((recordID = uSelect.next()).length() > 0) {
				uFile.setRecordID(recordID);
				for (String column : columns) {
					JSONObject col = new JSONObject();
					col.put(column, uFile.readNamedField(column).toString());
					result.add(col);
				}
			}
			response.put("result", result);
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniFileException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} catch (UniSelectListException e) {
			errors.add(e.getMessage());
			HydraService.writeLog(e.getMessage());
		} finally {
			if (uFile != null) {
				try {
					uFile.close();
				} catch (UniFileException e) {
					errors.add(e.getMessage());
					HydraService.writeLog(e.getMessage());
				}
			}
		}
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject delete(String object, String selection) {
		JSONObject response = new JSONObject();
		JSONArray errors = new JSONArray();
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject subroutine(String object, String[] values) {
		JSONObject response = new JSONObject();
		JSONArray vals = new JSONArray();
		try {
			System.out.println("subroutine:"+object);
			System.out.println("arguments:"+values.length);
			System.out.println("session isActive:" + mSession.isActive());
			UniSubroutine subr = mSession.subroutine(object, values.length);
			for (int i = 0, l = values.length; i < l; i++)
				subr.setArg(i, values[i]);
			System.out.println("call");
			subr.call();
			for (int i = 0, l = values.length; i < l; i++)
				vals.add(subr.getArg(i));
			response.put("result", vals);
		} catch (UniSessionException e) {
			e.printStackTrace();
			HydraService.writeLog(e.getMessage());
		} catch (UniSubroutineException e) {
			e.printStackTrace();
			HydraService.writeLog(e.getMessage());
		}
		return response;
	}

}
