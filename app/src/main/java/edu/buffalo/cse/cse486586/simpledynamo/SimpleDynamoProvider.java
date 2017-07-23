package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String[] port = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	static final String keyAtt = "key";
	static final String valueAtt = "value";

	private static String myPort;
	private static String mPredPort;
	private static String mPredId;
	private static String nodeId;
	private static int clientNumb;
	private static int versionCounter;

	private static boolean wait;
	private static boolean inswait;
	private static int queryCounter;
	private static boolean recover = true;
	private static boolean receiveStorage1 = false;
	private static boolean receiveStorage2 = false;
	private static boolean receiveReplica = false;

	private static final int DELETALL = 0;
	private static final int INSERT = 3;
	private static final int DELETE = 4;
	private static final int QUERY = 5;
	private static final int RETURNQUERY = 6;
	private static final int ALLEND = 7;
	private static final int INSREPLICA = 8;
	private static final int DELREPLICA = 9;
	private static final int QUERYALL = 10;
	private static final int REQUESTSTORAGE = 11;
	private static final int REQUESTREPLICA = 12;
	private static final int RETURNSTORAGE = 13;
	private static final int RETURNREPLICA = 14;
	private static final int QUERYREPLICA = 15;
	private static final int QUERYALLREPLICA = 16;

	Map<String, String> storage = new ConcurrentHashMap<String, String>();
	Map<String, String> all = new ConcurrentHashMap<String, String>();
	Map<String, String> replica = new ConcurrentHashMap<String, String>();
	Map<String, String> preferList = new LinkedHashMap<String, String>();
	ConcurrentMsg msg = new ConcurrentMsg();
	List<String> others = new ArrayList<String>();
	Map<String, String> otherRep = new ConcurrentHashMap<String, String>();
	Map<String, String> otherSto = new ConcurrentHashMap<String, String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		delete(selection, DELETE);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		synchronized (this) {
			insert(values.getAsString(keyAtt), values.getAsString(valueAtt), myPort);
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.v("myPort", myPort);

		Log.v("recover", Boolean.toString(recover));

		wait = false;
		inswait = false;
		clientNumb = 5;
		queryCounter = 0;
		versionCounter = 0;
		genPreferList(portStr);
		for(Map.Entry<String, String> s: preferList.entrySet()) {
			if(!s.getKey().equals(myPort))
				others.add(s.getKey());
		}
		Log.v("others", others.get(0) + "_" + others.get(1) + "_" + others.get(2) + "_" + others.get(3));
		try {
			nodeId = genHash(portStr);
			mPredId = genHash(Integer.toString(Integer.parseInt(mPredPort) / 2));
			Log.v("nodePort_predPort", portStr + "_" + mPredPort);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		try {
			ServerSocket serverS = new ServerSocket(SERVER_PORT);
			serverThread(serverS);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(recover) {
			Log.v("recover judge","recover is true");
			try {
				synchronized (this) {
					clientThread(others.get(2), null, null, myPort, null, null, REQUESTSTORAGE, null);
					clientThread(others.get(3), null, null, myPort, null, null, REQUESTSTORAGE, null);
					clientThread(others.get(0), null, null, myPort, null, null, REQUESTREPLICA, null);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.v("start query key = ", selection);
		Cursor cursor;
		synchronized (this) {
			cursor = query(selection, null, myPort);
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private void recoverThread() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {

					while (true) {
						if(receiveStorage1 && receiveStorage2 && receiveReplica) {
							for(Map.Entry<String, String> en: otherSto.entrySet()) {
								if(otherRep.containsKey(en.getKey()))
									otherRep.remove(en.getKey());
							}
							storage.putAll(otherRep);
							Log.v("oncreate", "receive storage and replica");
							recover = false;
							receiveReplica = false;
							receiveStorage2 = false;
							receiveStorage1 = false;
							break;
						}
					}

				} catch (Exception e) {
					Log.e(TAG, "onCreate clientThread Exception");
				}
			}
		}).start();
	}

	private void serverThread(final ServerSocket socket) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				//synchronized (this) {
					ServerSocket server = socket;
					ObjectInputStream objIS = null;
					Socket clientSocket = null;
					try {
						while (true) {
							Message message = null;
							try {
								clientSocket = server.accept();
								objIS = new ObjectInputStream(clientSocket.getInputStream());
								message = (Message) objIS.readObject();

								if (message.type == INSERT) {
									Log.v("receive INSERT", message.nodeId);
									storage.put(message.nodeId, message.predPort);

								} else if (message.type == INSREPLICA) {
									Log.v("receive REPLICA", message.nodeId);

									replica.put(message.nodeId, message.predPort);
								} else if (message.type == DELREPLICA) {
									Log.v("receive DELREPLICA", message.nodeId);
									delete(message.nodeId, message.type);

								} else if (message.type == DELETE) {
									Log.v("receive DELETE", message.nodeId);
									delete(message.nodeId, message.type);
								} else if (message.type == DELETALL) {
									Log.v("receive DELETEALL", message.nodeId);
									delete(message.nodeId, message.type);
								} else if (message.type == QUERY) {
									if (message.nodeId.equals("*")) {
										all.clear();
										all.putAll(message.map);
									}
									Log.v("receive QUERY", message.succPort + "_" + message.nodeId);
									synchronized (this) {
										query(message.nodeId, null, message.succPort);
									}
								} else if (message.type == RETURNQUERY) {
									synchronized (msg) {
										if (msg.version.containsKey(message.nodeId)) {
											Log.v("version containsKey", message.predPort);
											versionCounter++;
											msg.version.get(message.nodeId).add(message.predPort);
											if (versionCounter >= 2) {
												msg.notify();
												Log.v("versionCounter is 2", message.nodeId);
											}
											Log.v("receive RETURNQUERY", message.nodeId + "_" + message.predPort);
										}

									}
								} else if (message.type == QUERYALL) {
									Log.v("receive QUERYALL", "10");
									Log.v("ALLEND des", message.succPort);
									clientThread(message.succPort, message.nodeId, null, null, null, null, ALLEND, storage);
								} else if (message.type == QUERYREPLICA) {
									Log.v("receive QUERYREPLICA", message.succPort + "_" + message.nodeId);
									if (replica.containsKey(message.nodeId) && (replica.get(message.nodeId) != null))
										clientThread(message.succPort, message.nodeId, replica.get(message.nodeId), myPort, null, null, RETURNQUERY, null);
									else if (replica.get(message.nodeId) == null)
										Log.v("replica null mapping", message.nodeId);
								} else if (message.type == QUERYALLREPLICA) {
									Log.v("receive QUERYALLREPLICA", message.succPort);
									clientThread(message.succPort, message.nodeId, null, null, null, null, ALLEND, replica);
								} else if (message.type == ALLEND) {
									Log.v("receive ALLEND", "7");
									all.putAll(message.map);
									queryCounter++;
									Log.v("queryCounter", myPort + "_" + Integer.toString(queryCounter));

								} else if (message.type == REQUESTSTORAGE) {
									Log.v("receive REQUESTSTORAGE", message.succPort);
									clientThread(message.succPort, null, null, myPort, null, null, RETURNSTORAGE, storage);
								} else if (message.type == REQUESTREPLICA) {
									Log.v("receive REQUESTREPLICA", message.succPort);
									clientThread(message.succPort, null, null, myPort, null, null, RETURNREPLICA, replica);
								} else if (message.type == RETURNSTORAGE) {
									Log.v("receive RETURNSTORAGE", message.succPort);
									if (message.succPort.equals(others.get(2))) {

										replica.putAll(message.map);
										receiveStorage2 = true;

									} else if (message.succPort.equals(others.get(3))) {

										replica.putAll(message.map);
										otherSto.putAll(message.map);
										receiveStorage1 = true;

									}
								} else if (message.type == RETURNREPLICA) {
									Log.v("receive RETURNREPLICA", message.succPort);
									otherRep.putAll(message.map);
									receiveReplica = true;
									recoverThread();

								}
							} catch (IOException e) {
								Log.e(TAG, "serverSocket inner Exception");
							} finally {
								objIS.close();
								clientSocket.close();
							}

						}
					} catch (IOException e) {
						Log.e(TAG, "ServerSocket IOException");
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				//}
			}
		}).start();
	}

	private void clientThread(final String port, final String nodeId, final String predPort, final String succPort, final String predId, final String succId, final int type, final Map<String, String> all) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				synchronized (this) {
					Socket socket = null;
					ObjectOutputStream objOS = null;
					try {
						String remotePort = port;
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));
						Message message = new Message(predPort, nodeId, predPort, succPort, predId, succId, type, all);
						objOS = new ObjectOutputStream(socket.getOutputStream());
						objOS.writeObject(message);

						Log.v("client write", message.nodeId + "_" + message.predPort + "_" + type);
						Thread.sleep(30);
						objOS.flush();
						objOS.close();
						socket.close();
					} catch (Exception e) {

						if (type == QUERYALL) {
							String comPort = findSuccPort(port);
							Log.v("IOException queryall", port + "_" + comPort);
							try {
								objOS.close();
								socket.close();
							} catch (IOException e1) {
								e1.printStackTrace();
							}

							clientThread(comPort, nodeId, predPort, succPort, predId, succId, QUERYALLREPLICA, null);
						}

						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	private void genPreferList(String mPort) {
		ArrayList<String> temp = new ArrayList<String>();
		for(int i = 0;i < 5;i++)temp.add(port[i]);
		Collections.sort(temp, new Comparator<String>() {
			public int compare(String s1, String s2) {
				BigInteger b1 = BigInteger.valueOf(0);
				BigInteger b2 = BigInteger.valueOf(0);
				try {
					b1 = new BigInteger(genHash(s1), 16);
					b2 = new BigInteger(genHash(s2), 16);

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} finally {
					return b1.compareTo(b2);
				}
			}
		});
		int mIndex = temp.indexOf(mPort);
		if(mIndex > 0)mPredPort = Integer.toString(Integer.parseInt(temp.get(mIndex-1)) * 2);
		else mPredPort = Integer.toString(Integer.parseInt(temp.get(clientNumb-1)) * 2);
		int c = 0;
		int j = mIndex;
		while(c < 5) {
			try {
				preferList.put(Integer.toString(Integer.parseInt(temp.get(j)) * 2), genHash(temp.get(j)));
				Log.v("genPreferList", Integer.toString(j) + "_" + temp.get(j) + "_" + genHash(temp.get(j)));
				c++;
				j++;
				if(j >= 5)j = 0;
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean blToThisNode(String id, String PredId, String noId) {
		BigInteger nid = new BigInteger(id, 16);
		BigInteger mpi = new BigInteger(PredId, 16);
		BigInteger mid = new BigInteger(noId, 16);

		Log.v("blToThisNode", nid.toString() + "_" + mpi.toString() + "_" + mid.toString());
		if(mpi.compareTo(mid)!= 0) {
			if (nid.compareTo(mpi) == 1 && nid.compareTo(mid) != 1) {
				Log.v("blToThisNode", "btween true");
				return true;
			}
			else if(mid.compareTo(mpi) == -1 && (nid.compareTo(mpi) == 1 || nid.compareTo(mid) == -1)) {
				Log.v("blToThisNode", "btween -1 true");
				return true;
			}
		}
		else {
			Log.v("blToThisNode", "mpi = mid true");
			return true;
		}
		Log.v("blToThisNode", "false");
		return false;
	}

	private String findSuccPort(String thisPort) {
		String succPort = "";
		int mIndex = others.indexOf(thisPort);
		if(mIndex < 3)
			succPort = others.get(mIndex + 1);
		else
			succPort = myPort;
		return succPort;
	}

	private String findVersion(String s1, String s2, String s3) {
		String max = s1;
		if(s2.equals(s3))
			max = s2;
		return max;
	}

	private void insert(String key, String value, String sourcePort) {
		try {
			String hashKey = genHash(key);
			Log.v("insert hashKey", hashKey);
			synchronized (msg) {
				if (blToThisNode(hashKey, mPredId, nodeId)) {
						Log.v("insert to origin node", key + "_" + value);
						storage.put(key, value);

						Map<String, String> tempMap;
						synchronized (preferList) {
							tempMap = new LinkedHashMap<String, String>(preferList);
						}
						Iterator it = tempMap.entrySet().iterator();
						Map.Entry en = (Map.Entry) it.next();
						int succCounter = 0;
						String tPredId = (String) en.getValue();
						Log.v("send replica sourcePort", (String) en.getKey());
						while (it.hasNext() && succCounter < 2) {
							Map.Entry fn = (Map.Entry) it.next();
							Log.v("clientThread invoke5", "4");
							clientThread((String) fn.getKey(), key, value, null, null, null, INSREPLICA, null);
							Log.v("replica port", key + "_" + fn.getKey());
							succCounter++;
						}


				} else {
						Log.v("find nodes to insert", key + "_" + value);
						Map<String, String> tempMap;
						synchronized (preferList) {
							tempMap = new LinkedHashMap<String, String>(preferList);
						}
						Iterator it = tempMap.entrySet().iterator();
						Map.Entry en = (Map.Entry) it.next();
						String tPredId = (String) en.getValue();
						int sent = 0;
						boolean foundDes = false;
						while (it.hasNext() && sent < 3) {
							Map.Entry fn = (Map.Entry) it.next();
							String nid = (String) fn.getValue();
							if (!foundDes) {
								if (blToThisNode(hashKey, tPredId, nid)) {
									Log.v("found the node", key + "_" + (String) fn.getKey());
									clientThread((String) fn.getKey(), key, value, myPort, null, null, INSERT, null);
									sent++;
									foundDes = true;
								} else tPredId = nid;
							} else {
								clientThread((String) fn.getKey(), key, value, myPort, null, null, INSREPLICA, null);
								Log.v("find repNode1", key + "_" + (String) fn.getKey());
								sent++;
							}

						}
						if (sent < 3) {
							Map<String, String> tMap;
							synchronized (preferList) {
								tMap = new LinkedHashMap<String, String>(preferList);
							}
							for (Map.Entry<String, String> se : tMap.entrySet()) {
								clientThread(se.getKey(), key, value, myPort, null, null, INSREPLICA, null);
								Log.v("find repNode2", key + "_" + se.getKey());
								sent++;
								if (sent >= 3) break;
							}
						}
						sent = 0;
						foundDes = false;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private void delete(String selection, int type) {
		if (selection.equals("*")) {
			if(type == DELETE) {
				Log.v("delete *", "");
				if (!storage.isEmpty())
					storage.clear();

				if (!replica.isEmpty())
					replica.clear();

				Iterator it = preferList.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry en = (Map.Entry) it.next();
					clientThread((String) en.getKey(), selection, null, null, null, null, DELETALL, null);
				}
			}
			else if(type == DELETALL) {
				Log.v("delete ALL *", "");
				if (!storage.isEmpty())
					storage.clear();

				if (!replica.isEmpty())
					replica.clear();
			}
		}
		else if(selection.equals("@") && !storage.isEmpty()) {
			if(type == DELETE) {
				Log.v("delete @", "");
				storage.clear();
				Iterator it = preferList.entrySet().iterator();
				Map.Entry en = (Map.Entry) it.next();
				int succCounter = 0;
				String tPredId = (String) en.getValue();
				while (it.hasNext() && succCounter < 2) {
					Map.Entry fn = (Map.Entry) it.next();
					Log.v("clientThread invoke5", "6");
					clientThread((String) fn.getKey(), selection, null, null, null, null, DELREPLICA, null);
					succCounter++;
				}
			}
			else if(type == DELREPLICA) {
				replica.clear();
			}
		}
		else if(type == DELREPLICA) {

			if(!replica.isEmpty() && replica.containsKey(selection))
				replica.remove(selection);

			if(!storage.isEmpty() && storage.containsKey(selection))
				storage.remove(selection);
		}
		else {

			Log.v("normal delete", "");
			try {
				String hashDelete = genHash(selection);
				if(blToThisNode(hashDelete, mPredId, nodeId)) {
					if(storage.containsKey(selection)) {
						Log.v("normal delete remove", selection);
						storage.remove(selection);
						Iterator it = preferList.entrySet().iterator();
						Map.Entry en = (Map.Entry)it.next();
						int succCounter = 0;
						String tPredId = (String) en.getValue();
						while (it.hasNext() && succCounter < 2) {
							Map.Entry fn = (Map.Entry)it.next();
							Log.v("clientThread invoke5", "6");
							clientThread((String)fn.getKey(), selection, null, null, null, null, DELREPLICA, null);
							succCounter++;
						}
					}
					else
						Log.v("normal delete", "blong but nothing to delete");
				}
				else {
					Log.v("find node to delete", "nomal");
					Log.v("clientThread invoke7", "7");
					Iterator it = preferList.entrySet().iterator();
					Map.Entry en = (Map.Entry)it.next();
					String tPredId = (String) en.getValue();
					while (it.hasNext()) {
						Map.Entry fn = (Map.Entry)it.next();
						String nid = (String)fn.getValue();
						if(blToThisNode(hashDelete, tPredId, nid)) {
							clientThread((String)fn.getKey(), selection, null, null, null, null, DELETE, null);
							break;
						}
						else tPredId = nid;
					}

				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
	}

	private  Cursor query(String selection, String value, String sourcePort) {

		MatrixCursor cur = new MatrixCursor(new String[]{keyAtt, valueAtt});

		if (selection.equals("*")) {
			Log.v("* query", myPort);
			all.putAll(storage);

			Map<String, String> tempMap;
			synchronized (preferList) {
				tempMap = new LinkedHashMap<String, String>(preferList);
			}

			for (Map.Entry<String, String> entry : tempMap.entrySet()) {
				if (entry.getKey().equals(myPort)) continue;
				clientThread(entry.getKey(), selection, null, myPort, null, null, QUERYALL, null);
				Log.v("queryall sent", entry.getKey());
			}

			while (queryCounter < 4) {
			}
			Log.v("query returned", Integer.toString(queryCounter));
			for (Map.Entry<String, String> entry : all.entrySet()) {
				cur.addRow(new String[]{entry.getKey(), entry.getValue()});
				Log.v("all end query", entry.getKey() + "_" + entry.getValue());
			}
			all.clear();
			queryCounter = 0;

		} else if (selection.equals("@")) {
			if(sourcePort.equals(myPort)) {
				Log.v("@ query", "sourcePort");

				all.putAll(storage);
				all.putAll(replica);

				for (Map.Entry<String, String> entry : all.entrySet()) {
					cur.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
				all.clear();
			}
		} else {
			Log.v("normal query", selection);
			try {
				String suc1 = "";
				String suc2;
				String hashQuery = genHash(selection);
				synchronized (msg) {
					if (blToThisNode(hashQuery, mPredId, nodeId)) {
							if (sourcePort.equals(myPort)/* && storage.containsKey(selection)*/) {
								Log.v("normal query in source", selection);
								msg.version.put(selection, new ArrayList<String>());
								if(storage.containsKey(selection)) {
									Log.v("query value", storage.get(selection));

									versionCounter++;

									msg.version.get(selection).add(storage.get(selection));
								}
								suc1 = findSuccPort(sourcePort);
								suc2 = findSuccPort(suc1);
								clientThread(suc1, selection, value, sourcePort, null, null, QUERYREPLICA, null);
								clientThread(suc2, selection, value, sourcePort, null, null, QUERYREPLICA, null);

								msg.wait(6000);
								if(!msg.version.isEmpty() && !msg.version.get(selection).isEmpty()) {
									Log.v("receiveAllQuery", selection + "_" + msg.version.get(selection).get(0));
									msg.setValue(msg.version.get(selection).get(0));
									msg.setKey(selection);
									cur.addRow(new String[]{selection, msg.getValue()});
									msg.version.remove(selection);
									versionCounter = 0;
								}
								else
									Log.v("msg.version is empty", selection);

							} else if (storage.containsKey(selection) && storage.get(selection) != null) {
								Log.v("query not in source", selection);
								Log.v("clientThread invoke11", "11");
								clientThread(sourcePort, selection, storage.get(selection), sourcePort, null, null, RETURNQUERY, null);
								Log.v("RETURNQUERY", selection + "_" + storage.get(selection));

							}
					} else {
							Log.v("find node to query", selection);
							Iterator it = preferList.entrySet().iterator();
							Map.Entry en = (Map.Entry) it.next();
							String tPredId = (String) en.getValue();
							while (it.hasNext()) {
								Map.Entry fn = (Map.Entry) it.next();
								String nid = (String) fn.getValue();
								if (blToThisNode(hashQuery, tPredId, nid)) {
									Log.v("clientThread invoke12", "12");
									msg.version.put(selection, new ArrayList<String>());
									clientThread((String) fn.getKey(), selection, null, sourcePort, null, null, QUERY, null);
									suc1 = findSuccPort((String) fn.getKey());
									break;
								} else tPredId = nid;
							}
							suc2 = findSuccPort(suc1);
							clientThread(suc1, selection, value, sourcePort, null, null, QUERYREPLICA, null);
							clientThread(suc2, selection, value, sourcePort, null, null, QUERYREPLICA, null);

							if (myPort.equals(sourcePort)) {

								Log.v("find node to query", "source wait");

								msg.wait(6000);
								if(!msg.version.isEmpty() && !msg.version.get(selection).isEmpty()) {

									Log.v("receiveAllQuery", selection + "_" + msg.version.get(selection).get(0));
									msg.setValue(msg.version.get(selection).get(0));
									msg.setKey(selection);

									String rk = msg.getKey();
									String rv = msg.getValue();
									Log.v("rk_rv", rk + "_" + rv);
									cur.addRow((new String[]{rk, rv}));
									msg.version.remove(selection);
									versionCounter = 0;
								}
								else
									Log.v("msg.version is empty", selection);

							}
					}
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return cur;
	}


}
