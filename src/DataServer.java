import java.net.*;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.SimpleDateFormat;

public class DataServer {

	static ArrayList<String> passwords = new ArrayList<String>();
	static ArrayList<HashMap<String, String>> queries = new ArrayList<HashMap<String, String>>();
	static HashMap<String, String> databaseIndexes = new HashMap<String, String>();
	static int currentQueryIndex = 0;
	
	public static void main(String[] args) throws Exception {

		System.out.println("Data Server Started.");

		/* ------------------------------TODO--------------------------------- */
		/* --- 1. Create a instance on Nectar Cloud for a worker ------------- */
		/* --- 2. Set a limited storage capacity for the worker -------------- */
		/* --- 3. Once the storage limit is reached, Create a new instance --- */
		/* --- Now we only have a local woker with port 9001 and 9002. ------- */
		/* --- We can finally do this feature. ------------------------------- */
		// Worker to store Tweets data
		new Thread(new Runnable() {
			public void run() {
				try {
					// waiting the Tweet generator server
					Socket s_generator = new Socket(InetAddress.getLocalHost(), 9099);

					// run the first worker
					processFile("/Users/Jason/Github/DataServer/src/Worker1.jar");
					// open port 9000 for worker1 to save tweets
					ServerSocket ss_worker1 = new ServerSocket(9000);
					Socket s_worker1 = ss_worker1.accept();
					System.out.println("Worker1 Database service starts to run.");

					// open port 9001 for worker1 to handle query
					handleQueryThreadStart(new ServerSocket(9001), 0);

					// start inserting data into the worker1
					insertTweets(s_worker1, s_generator, 10, 0);
					System.out.println("Worker1 has reached the storage limit.");

					// close socket and server socket on port 9000
					s_worker1.close();
					ss_worker1.close();
					System.out.println("Worker1 Database service is over.");

					// run the second worker
					processFile("/Users/Jason/Github/DataServer/src/Worker2.jar");
					// open port 9002 for worker2 to save tweets
					ServerSocket ss_worker2 = new ServerSocket(9002);
					Socket s_worker2 = ss_worker2.accept();
					System.out.println("Worker2 Database service starts to run.");

					// open port 9003 for worker2 to handle query
					handleQueryThreadStart(new ServerSocket(9003), 1);

					// start inserting data into the worker2
					insertTweets(s_worker2, s_generator, 10, 1);
					System.out.println("Worker2 has reached the storage limit.");

					// close socket and server socket on port 9002
					s_worker2.close();
					ss_worker2.close();
					System.out.println("Worker2 Database service is over.");

				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}).start();

		// Clients to make a query
		ServerSocket ss_query = new ServerSocket(9098);
		int counter = 0;
		while (true) {
			counter++;
			Socket s_query = ss_query.accept(); // server accept the client connection request
			System.out.println(" >> " + "Client No:" + counter + " started!");

			new Thread(new Runnable() {
				public void run() {
					communicateWithClients(s_query);
				}
			}).start();
		}
	}

	public static void insertTweets(Socket s_worker, Socket s_generator, int maximumNumber, int index) {
		try {
			DataOutputStream out = new DataOutputStream(s_worker.getOutputStream());

			// receive tweet from Data Stream Generator

			int counter = 0;
			while (counter < maximumNumber) {
				// Read a tweet string from the server
				DataInputStream in = new DataInputStream(s_generator.getInputStream());
				String tweetStr = in.readUTF();
				String[] tweet = tweetStr.split("\t");

				out.writeUTF(tweet[0] + "	" + tweet[1] + "	" + tweet[5] + "	" + tweet[10] + "	" + tweet[12]);
				counter++;
				System.out.println("Tweet saved" + "(" + counter + "/" + maximumNumber + "): " + tweet[0] + "	"
						+ tweet[1] + "	" + tweet[5] + "	" + tweet[10] + "	" + tweet[12]);

				// index: bind index(0 or 1) to a tweet ID for future query executions
				if (index == 0) {
					databaseIndexes.put(tweet[0], "0");
				} else {
					databaseIndexes.put(tweet[0], "1");
				}
			}

			out.close();

		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void handleQuery(Socket s, int index) {

		int counter = 0;

		try {

			System.out.println("Worker" + (index + 1) + " Handle Query service starts to run.");

			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());

			while (true) {

				int size;
				synchronized (queries) {
					size = queries.size();
				}

				if (size > counter) {
					
					currentQueryIndex = counter + 1;
					HashMap<String, String> query = queries.get(counter);
					String resultString = "";

					// If index == "", means both workers should not do the query
					if (query.get("index").length() > 0) {

						// If current tweet is stored in worker1(index=0) and the query type are 1 or 4
						// If current tweet is stored in worker2(index=1) and the query type are 1 or 4
						// Then only one worker should do the query
						if ((index == 0 && query.get("index").equalsIgnoreCase("0"))
								|| (index == 1 && query.get("index").equalsIgnoreCase("1"))) {
							out.writeUTF(query.get("queryType") + "	" + query.get("text"));
							resultString = in.readUTF();

							HashMap<String, String> result = queries.get(counter);
							result.put("result", resultString);
							System.out.println(
									"Query" + query.get("queryID") + " execution ends, the result is: " + resultString);

						}

						// If the query type are 2 or 3
						// Then both workers should do the query
						else if (query.get("index").equalsIgnoreCase("2")) {
							out.writeUTF(query.get("queryType") + "	" + query.get("text"));
							resultString = in.readUTF();
							String[] resultStringArray_new = resultString.split("\t");
							String timeConsumingString_new = resultStringArray_new[0];
							String totalNumberString_new = resultStringArray_new[1];

							HashMap<String, String> result = queries.get(counter);

							synchronized (queries) {
								if (result.get("result").equalsIgnoreCase("processing") ||
									result.get("result").equalsIgnoreCase("uncompleted")) {
									resultString = Double.parseDouble(timeConsumingString_new) + "	"
											+ totalNumberString_new;
									
									result.put("result", resultString);

									System.out.println("Query" + query.get("queryID")
									+ " execution ends, the result is: " + resultString);
								} else {
									String[] resultStringArray_old = result.get("result").split("\t");
									String timeConsumingString_old = resultStringArray_old[0];
									String totalNumberString_old = resultStringArray_old[1];
									Double totalTimeConsuming = Double.parseDouble(timeConsumingString_new)
											+ Double.parseDouble(timeConsumingString_old);
									int totalResult = Integer.parseInt(totalNumberString_new)
											+ Integer.parseInt(totalNumberString_old);
									resultString = totalTimeConsuming + "	" + totalResult;

									result.put("result", resultString);
									
									System.out.println("Query" + query.get("queryID")
											+ " execution ends, the result is: " + resultString);

								}
							}
						}
					}
					counter++;
				}
			}

		} catch (IOException e) {
			HashMap<String, String> result = queries.get(counter);

			// If any worker is closed, change the result to "uncompleted".
			// If any alive worker still handling the query, the result will change back to a partial result.
			if (result.get("result").equalsIgnoreCase("processing")) {
				result.put("result", "uncompleted");
			}
			System.out.println("Worker" + (index + 1) + " is closed unexpectedly. QueryID" + result.get("queryID")
					+ " is forced to end. The current result is " + result.get("uncompleted"));

			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void communicateWithClients(Socket s) {
		try {
			while (true) {
				// Instantiate input and output streams
				DataInputStream in = new DataInputStream(s.getInputStream());
				DataOutputStream out = new DataOutputStream(s.getOutputStream());

				// Send a menu to the client
				// Welcome to DataServer, please type a number to access a service:
				// 1 to register a new password
				// 2 to access services via a password
				// 3 to exit
				out.writeUTF("Welcome to DataServer, please type a number to access a service:");
				out.writeUTF("1 to register a new password");
				out.writeUTF("2 to access services via a password");
				out.writeUTF("3 to exit");

				// Read an option from the client
				String option = in.readUTF();
				String password;

				if (option.equalsIgnoreCase("1")) {

					password = Integer.toString(passwords.size());
					out.writeUTF("Your passcode is " + password);
					passwords.add(password);

				} else if (option.equalsIgnoreCase("2")) {

					// Send "Enter your password:" to the client
					out.writeUTF("Enter your password:");
					while (true) {
						// Read a password from the client
						password = in.readUTF();
						// Send "Connection Success" or "Invalid Password, please enter your password
						// again:" to the client
						if (passwords.contains(password)) {
							out.writeUTF("Connection Success");
							break;
						} else {
							out.writeUTF("Invalid Password, please enter your password again:");
						}
					}
					while (true) {
						// Send a menu to the client
						// Success to access your services, please type a number to access a service:
						// 1 to search a text by a tweet ID
						// 2 to search a number of tweets containing a specific words
						// 3 to search a number of tweets from a specific airline
						// 4 to find the most frequent character in a tweet by a tweet ID
						// 5 to get a status/result by a query ID
						// 6 to exit
						out.writeUTF("Success to access your services, please type a number to access a service:");
						out.writeUTF("1 to search a text by a tweet ID");
						out.writeUTF("2 to search a number of tweets containing a specific words");
						out.writeUTF("3 to search a number of tweets from a specific airline");
						out.writeUTF("4 to find the most frequent character in a tweet by a tweet ID");
						out.writeUTF("5 to get a status/result by a query ID");
						out.writeUTF("6 to cancel a query by a query ID");
						out.writeUTF("7 to back");

						// Read an option from the client
						option = in.readUTF();

						if (option.equalsIgnoreCase("1")) {

							out.writeUTF("Enter a tweet ID:");
							String queryID = insertQuery(in.readUTF(), password, 1);
							out.writeUTF("Your query ID is " + queryID + ".");

						} else if (option.equalsIgnoreCase("2")) {

							out.writeUTF("Enter a words:");
							String queryID = insertQuery(in.readUTF(), password, 2);
							out.writeUTF("Your query ID is " + queryID + ".");

						} else if (option.equalsIgnoreCase("3")) {

							out.writeUTF("Enter an airline:");
							String queryID = insertQuery(in.readUTF(), password, 3);
							out.writeUTF("Your query ID is " + queryID + ".");

						} else if (option.equalsIgnoreCase("4")) {

							out.writeUTF("Enter a tweet ID:");
							String queryID = insertQuery(in.readUTF(), password, 4);
							out.writeUTF("Your query ID is " + queryID + ".");

						} else if (option.equalsIgnoreCase("5")) {

							out.writeUTF("Enter a query ID:");
							String result = getResult(in.readUTF(), password);
							out.writeUTF(result);

						} else if (option.equalsIgnoreCase("6")) {

							out.writeUTF("Enter a query ID:");
							String result = cancelQuery(in.readUTF(), password);
							out.writeUTF(result);

						} else if (option.equalsIgnoreCase("7")) {
							break;
						}
					}
				} else if (option.equalsIgnoreCase("3")) {
					out.writeUTF("You have been disconnected.");
					break;
				}
			}

		} catch (NullPointerException e) {
			System.out.println("Exception: Client Disconnected");
		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	/**
	 * 
	 * @param queryID
	 * @param password
	 * @return
	 */
	public static String getResult(String queryID, String password) {
		String text = "Invalid query ID";
		for (int i = 0; i < queries.size(); i++) {
			HashMap<String, String> result = queries.get(i);
			if (queryID.equalsIgnoreCase(result.get("queryID")) && password.equalsIgnoreCase(result.get("password"))) {
				text = result.get("result");
				break;
			}
		}
		return text;
	}
	
	/**
	 * 
	 * @param queryID
	 * @param password
	 * @return
	 */
	public static String cancelQuery(String queryID, String password) {
		String text = "Invalid query ID";
				
		for (int i = 0; i < queries.size(); i++) {			
			HashMap<String, String> query = queries.get(i);
			if (queryID.equalsIgnoreCase(query.get("queryID")) && password.equalsIgnoreCase(query.get("password"))) {
				
				// 2 > 
				if (currentQueryIndex > i) {
					text = "Query" + query.get("queryID") + " is processing or has been processed, cannot be cancelled.";
				} else {
					text = "Query" + query.get("queryID") + " cancelled.";
					queries.remove(i);
					System.out.println(text);
				}

				break;
			}
		}
		return text;
	}

	/**
	 * 
	 * @param text
	 * @param password
	 * @param type
	 * @return
	 */
	public static String insertQuery(String text, String password, int type) {
		/*
		 * --------------------------------------TODO
		 * 2-------------------------------------------------
		 */
		/*
		 * --- 1. Parse the text from the text value
		 * ---------------------------------------------------
		 */
		/*
		 * --- 2. Parse the deadline(time) from the text
		 * value------------------------------------------
		 */
		/*
		 * --- 3. If there is no deadline(time), add the query into queries
		 * ----------------------------
		 */
		/*
		 * --- 4. If there is a deadline(time), insert the query into queries in
		 * chronological order ---
		 */
		/*
		 * -----------------------------------------------------------------------------
		 * ----------------
		 */
		Map<String, String> query = new HashMap<String, String>();

		String queryID = Integer.toString(queries.size());
		String queryType = String.valueOf(type);
		String deadline = "";

		String[] textArray = text.split("\t");
		if (textArray.length == 2) {
			text = textArray[0];
			deadline = textArray[1];
		}

		query.put("queryID", queryID);
		query.put("deadline", deadline);
		query.put("queryType", queryType);
		query.put("password", password);
		query.put("result", "processing");
		query.put("text", text);

		if (queryType.equalsIgnoreCase("1") || queryType.equalsIgnoreCase("4")) {
			// index == "0" means only worker1 should do the query
			// index == "1" means only worker2 should do the query
			// index == "" means both workers should not do the query
			query.put("index", databaseIndexes.getOrDefault(text, ""));
			// if there is no tweetID in indexesHashMap, means there must be no result in
			// any databases, so give "no result" directly.

		} else {
			// index == "2" means both workers should do the query and need to put the
			// answers together
			query.put("index", "2");
		}
		
		// insert as a ergent query
		if (deadline.length() > 0 && queries.size() > currentQueryIndex) {
			for (int i = currentQueryIndex; i < queries.size(); i++) {
				if (queries.get(i).get("deadline").length() > 0) {
					if (Integer.parseInt(deadline) >= Integer.parseInt(queries.get(i).get("deadline"))) {
						continue;
					} else {
						queries.add(i, (HashMap<String, String>) query);
						System.out.println("New query added as a urgent query(current order:" + i + " " + "current deadline:<" + deadline + ">)" + query);
						break;
					}
				} else {
					queries.add(i, (HashMap<String, String>) query);
					System.out.println("New query added as a urgent query(current order:" + i + " " + "current deadline:<" + deadline + ">)" + query);
					break;
				}
			}
		}
		// insert as a normal query
		else {
			queries.add((HashMap<String, String>) query);
			
			if (deadline.length() > 0) {
				System.out.println("New query added as a urgent query(current order:" + (queries.size() - 1) + " " + "current deadline:<" + deadline + ">)" + query);
			} else {
				System.out.println("New query added as a normal query(current order:" + (queries.size() - 1) + " " + "current deadline:<" + deadline + ">)" + query);
			}
		}

		if (query.get("index").length() == 0) {
			query.put("result", text + " doesn't exist in any tweets");
			System.out.println("Query" + queryID + " execution ends, the result is: " + text + " doesn't exist in any tweets");
		}
		
		return queryID;
	}

	public static void handleQueryThreadStart(ServerSocket ss_worker, int index) {
		// Worker to handle the query
		new Thread(new Runnable() {
			public void run() {
				try {
					Socket s_worker = ss_worker.accept();
					handleQuery(s_worker, index);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	public static Process processFile(String path) {
		Process process = null;
		try {
			process = Runtime.getRuntime().exec("java -jar " + path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return process;
	}

	public static boolean canParseInt(String str) {
		if (str == null) {
			return false;
		}
		return str.matches("\\d+");
	}
}
