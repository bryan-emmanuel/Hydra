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

import java.util.ArrayList;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

//import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniclientlibs.UniTokens;
import asjava.uniobjects.UniCommand;
import asjava.uniobjects.UniCommandException;
import asjava.uniobjects.UniDictionary;
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

	public UnidataConnection(String hostName, int hostPort, String accountPath, String username, String password, String dasu, String dasp, String sqlenvinit) {
		super(hostName, hostPort, accountPath, username, password, dasu, dasp, sqlenvinit);
	}

	@Override
	public boolean connect() throws Exception {
		super.connect();
		if (mSession == null) {
			mSession = new UniSession();
			mSession.setHostName(mHostName);
			mSession.setHostPort(mHostPort);
			mSession.setAccountPath(mAccountPath);
			mSession.setUserName(mUsername);
			mSession.setPassword(mPassword);
			mSession.setConnectionString("udcs");
			mSession.connect();
			if ((mDASU != null) && (mDASP != null)) {
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
			String[] fmValues = uCommand.response().split(UniTokens.AT_FM);
			JSONArray rows = new JSONArray();
			for (String fmValue : fmValues) {
				JSONArray rowData = new JSONArray();
				String[] vmValues = fmValue.split(UniTokens.AT_VM);
				for (String vmValue : vmValues)
					rowData.add(vmValue);
				rows.add(rowData);
			}
			response.put("result", rows);
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
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
			JSONArray rows = new JSONArray();
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
				// flatten out multi-values
				int maxSize = 1;
				ArrayList<String[]> colArr = new ArrayList<String[]>();
				for (String column : columns) {
					String[] mvArr = uFile.readNamedField(column).toString().split(UniTokens.AT_VM);
					if (mvArr.length > maxSize)
						maxSize = mvArr.length;
					colArr.add(mvArr);
				}
				for (int row = 0; row < maxSize; row++) {
					JSONArray rowData = new JSONArray();
					for (int col = 0; col < columns.length; col++) {
						String[] mvArr = colArr.get(col);
						if (row < mvArr.length)
							rowData.add(mvArr[row]);
						else
							rowData.add("");
					}
					rows.add(rowData);
				}
			}
			response.put("result", rows);
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
		} catch (UniFileException e) {
			errors.add(e.getMessage());
		} catch (UniSelectListException e) {
			errors.add(e.getMessage());
		} finally {
			if (uFile != null) {
				try {
					uFile.close();
				} catch (UniFileException e) {
					errors.add(e.getMessage());
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
		// need the recordID, it may be sent as @ID, or as the Colleague I-descriptors
		String recordID = null;
		for (int c = 0; (c < columns.length) && (c < values.length); c++) {
			if (("@ID").equals(columns[c])) {
				recordID = values[c];
				break;
			}
		}
		ArrayList<String> keyNames = new ArrayList<String>();
		if (recordID == null) {
			// @ID wasn't sent, look for the Colleague I-descriptors
			ArrayList<Integer> keyLocs = new ArrayList<Integer>();
			try {
				UniCommand uCommand = mSession.command();
				uCommand.setCommand(String.format(SELECTION_QUERY_FORMAT, "DICT " + object, "SELECT DICT STUDENT.TERMS WITH LOC LIKE 'FIELD(@ID,...*...'").toString());
				UniSelectList uSelect = mSession.selectList(0);
				uCommand.exec();
				UniDictionary dict = mSession.openDict(object);
				UniString fieldID = null;
				Pattern keyPattern = Pattern.compile("^FIELD\\(@ID,\"\\*\",\\d+\\)");
				while ((fieldID = uSelect.next()).length() > 0) {
					dict.setRecordID(fieldID);
					String loc = dict.getLoc().toString();
					if (keyPattern.matcher(loc).matches()) {
						int keyLoc = Integer.parseInt(loc.substring(14, loc.length() - 1));
						if (!keyLocs.contains(keyLoc)) {
							keyLocs.add(keyLoc);
							keyNames.add(fieldID.toString());
						}
					}
				}
			} catch (UniSessionException e) {
				e.printStackTrace();
			} catch (UniCommandException e) {
				e.printStackTrace();
			} catch (UniFileException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UniSelectListException e) {
				e.printStackTrace();
			}
			int s = keyNames.size();
			if (s > 0) {
				// check if the keys are defined
				String[] keyParts = new String[s];
				int partsFound = 0;
				for (int k = 0; k < s; k++) {
					for (int c = 0; (c < columns.length) && (c < values.length); c++) {
						if (keyNames.get(k).equals(columns[c])) {
							keyParts[keyLocs.get(k)] = values[c];
							partsFound++;
							break;
						}
					}
				}
				if (partsFound < s)
					errors.add("key not defined");
				else {
					recordID = "";
					for (int k = 0; k < s; k++)
						recordID += keyParts[k];
				}
			}
		}
		if (recordID != null) {
			UniFile uFile = null;
			try {
				uFile = mSession.openFile(object);
			} catch (UniSessionException e) {
				e.printStackTrace();
				errors.add("error opening file: " + object + ": " + e.getMessage());
			}
			if (uFile != null) {
				boolean fieldsWritten = false;
				try {
					uFile.setRecordID(recordID);
				} catch (UniFileException e) {
					e.printStackTrace();
					fieldsWritten = false;
				}
				fieldsWritten = true;
				for (int c = 0, cl = columns.length; c < cl; c++) {
					if (c < values.length) {
						// don't try to write the Colleague I-descriptors
						if (!keyNames.contains(columns[c])) {
							try {
								uFile.writeNamedField(columns[c], values[c]);
							} catch (UniFileException e) {
								e.printStackTrace();
								fieldsWritten = false;
								errors.add("error writing field: " + columns[c] + "=" + values[c] + ": " + e.getMessage());
							}
						}
					}
				}
				if (fieldsWritten) {
					try {
						uFile.write();
					} catch (UniFileException e) {
						e.printStackTrace();
						errors.add("error writing file: " + e.getMessage());
					}
					JSONArray rows = new JSONArray();
					JSONArray rowData = new JSONArray();
					rows.add(rowData);
					response.put("result", rows);
				}
			}
			else
				errors.add("key not defined");
		} else
			errors.add("key not found");
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
				for (int c = 0, cl = columns.length; c < cl; c++) {
					if (c < values.length)
						uFile.writeNamedField(columns[c], values[c]);
				}
				uFile.write();
			}
			JSONArray rows = new JSONArray();
			JSONArray rowData = new JSONArray();
			rows.add(rowData);
			response.put("result", rows);
		} catch (UniSessionException e) {
			errors.add(e.getMessage());
		} catch (UniCommandException e) {
			errors.add(e.getMessage());
		} catch (UniFileException e) {
			errors.add(e.getMessage());
		} catch (UniSelectListException e) {
			errors.add(e.getMessage());
		} finally {
			if (uFile != null) {
				try {
					uFile.close();
				} catch (UniFileException e) {
					errors.add(e.getMessage());
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
		UniCommand uCommand = null;
		try {
			uCommand = mSession.command();
		} catch (UniSessionException e) {
			e.printStackTrace();
			errors.add("error getting command: " + e.getMessage());
		}
		if (uCommand != null) {
			UniFile uFile = null;
			try {
				uFile = mSession.openFile(object);
			} catch (UniSessionException e) {
				e.printStackTrace();
				errors.add("error opening file: " + object + ": " + e.getMessage());
			}
			if (uFile != null) {
				if (selection == null)
					uCommand.setCommand(String.format(SIMPLE_QUERY_FORMAT, object).toString());
				else
					uCommand.setCommand(String.format(SELECTION_QUERY_FORMAT, object, selection).toString());
				UniSelectList uSelect = null;
				try {
					uSelect = mSession.selectList(0);
				} catch (UniSessionException e) {
					e.printStackTrace();
				}
				if (uSelect != null) {
					boolean gotSelection = false;
					try {
						uCommand.exec();
						gotSelection = true;
					} catch (UniCommandException e) {
						e.printStackTrace();
						errors.add("error getting selection: " + e.getMessage());
					}
					if (gotSelection) {
						UniString recordID = null;
						try {
							recordID = uSelect.next();
						} catch (UniSelectListException e) {
							e.printStackTrace();
							errors.add("error getting next record id: " + e.getMessage());
						}
						while ((recordID != null) && (recordID.length() > 0)) {
							try {
								uFile.setRecordID(recordID);
								uFile.deleteRecord();
								uFile.write();
							} catch (UniFileException e) {
								e.printStackTrace();
							}
						}
						JSONArray rows = new JSONArray();
						JSONArray rowData = new JSONArray();
						rows.add(rowData);
						response.put("result", rows);
					}
				}
			}
		}
		response.put("errors", errors);
		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JSONObject subroutine(String object, String[] arguments) {
		JSONObject response = new JSONObject();
		try {
			UniSubroutine subr = mSession.subroutine(object, arguments.length);
			for (int i = 0, l = arguments.length; i < l; i++)
				subr.setArg(i, arguments[i]);
			subr.call();
			
			JSONArray rows = new JSONArray();
			int maxSize = 1;
			ArrayList<String[]> colArr = new ArrayList<String[]>();
			for (int i = 0, l = arguments.length; i < l; i++) {
				String[] mvArr = subr.getArg(i).toString().split(UniTokens.AT_VM);
				if (mvArr.length > maxSize)
					maxSize = mvArr.length;
				colArr.add(mvArr);
			}
			for (int row = 0; row < maxSize; row++) {
				JSONArray rowData = new JSONArray();
				for (int col = 0; col < arguments.length; col++) {
					String[] mvArr = colArr.get(col);
					if (row < mvArr.length)
						rowData.add(mvArr[row]);
					else
						rowData.add("");
				}
				rows.add(rowData);
			}
			response.put("result", rows);
		} catch (UniSessionException e) {
			e.printStackTrace();
		} catch (UniSubroutineException e) {
			e.printStackTrace();
		}
		return response;
	}

}
