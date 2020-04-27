import java.net.*;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.SimpleDateFormat;

public class DataServer {

	static ArrayList<String> passwords = new ArrayList<String>();
	static ArrayList<HashMap<String, String>> queries = new ArrayList<HashMap<String, String>>();

	public static void main(String[] args) throws Exception {
		
		System.out.println("Data Server Started ....");

		/* ------------------------------TODO--------------------------------- */
		/* --- 1. Create a instance on Nectar Cloud for a worker ------------- */
		/* --- 2. Set a limited storage capacity for the worker -------------- */
		/* --- 3. Once the storage limit is reached, Create a new instance --- */
		/* --- Now we only have a local woker with port 9001 and 9002. ------- */
		/* --- We can finally do this feature. ------------------------------- */
		ServerSocket ss_database = new ServerSocket(9001);
		ServerSocket ss_handle = new ServerSocket(9002);

		Socket s_database = ss_database.accept();
		Socket s_handle = ss_handle.accept();

		// Worker to store Tweets data
		new Thread(new Runnable() {
			public void run() {
				insertTweets(s_database);
			}
		}).start();
		
		// Worker to handle the query
		new Thread(new Runnable() {
			public void run() {
				handleQuery(s_handle);
			}
		}).start();

		// Clients to make a query
		ServerSocket ss_query = new ServerSocket(9003);
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

	public static void insertTweets(Socket s) {
		try {
			DataOutputStream out = new DataOutputStream(s.getOutputStream());

			// receive tweet from Data Stream Generator
			Socket client = new Socket(InetAddress.getLocalHost(), 9000);
			DataInputStream clientIn = new DataInputStream(client.getInputStream());
			
			int capacity = 1000;
			int counter = 0;
			while (counter < capacity) {
				// Read a tweet string from the server
				String str = clientIn.readUTF();
				String[] tweet = str.split("\t");

				out.writeUTF(tweet[0] + "	" + tweet[1] + "	" + tweet[5] + "	" + tweet[10] + "	" + tweet[12]);
				counter++;
			}
			// Close the streams and the client
			clientIn.close();
			client.close();

			out.close();
			s.close();

		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static void handleQuery(Socket s) {
		try {
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());

			int counter = 0;
			while (true) {
				synchronized (queries) {
					if (queries.size() > counter) {
						HashMap<String, String> query = queries.get(counter);
						out.writeUTF(query.get("queryType") + "	" + query.get("text"));
						query.put("result", in.readUTF());
						counter++;
					}
				}
			}			

		} catch (IOException e) {
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

					synchronized (passwords) {
						password = Integer.toString(passwords.size());
						out.writeUTF("Your passcode is " + password);
						passwords.add(password);
					}

				} else if (option.equalsIgnoreCase("2")) {

					// Send "Enter your password:" to the client
					out.writeUTF("Enter your password:");
					while (true) {
						// Read a password from the client
						password = in.readUTF();
						// Send "Connection Success" or "Invalid Password, please enter your password
						// again:" to the client
						synchronized (passwords) {
							if (passwords.contains(password)) {
								out.writeUTF("Connection Success");
								break;
							} else {
								out.writeUTF("Invalid Password, please enter your password again:");
							}
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
						out.writeUTF("6 to back");

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

	public static String getResult(String queryID, String password) {
		String result = "Invalid query ID";
		synchronized (queries) {
			for (int i = 0; i < queries.size(); i++) {
				HashMap<String, String> query = queries.get(i);
				if (queryID.equalsIgnoreCase(query.get("queryID")) && password.equalsIgnoreCase(query.get("password"))) {
					result = "Your result is " + query.get("result") + ".";
					break;
				}
			}
		}
		return result;
	}
	
	public static String insertQuery(String text, String password, int type) {
		/* --------------------------------------TODO 2------------------------------------------------- */
		/* --- 1. Parse the text from the text value --------------------------------------------------- */
		/* --- 2. Parse the deadline(time) from the text value------------------------------------------ */
		/* --- 3. If there is no deadline(time), add the query into queries ---------------------------- */
		/* --- 4. If there is a deadline(time), insert the query into queries in chronological order --- */
		/* --------------------------------------------------------------------------------------------- */
		Map<String, String> query = new HashMap<String, String>();
		String queryID;
		synchronized (queries) {
			queryID = Integer.toString(queries.size());
		}
		String result = "processing";
		String deadline = "";
		String queryType = String.valueOf(type);

		query.put("queryID", queryID);
		query.put("result", result);
		query.put("deadline", deadline);
		query.put("password", password);
		query.put("queryType", queryType);
		query.put("text", text);

		synchronized (queries) {
			queries.add((HashMap<String, String>) query);
			System.out.println("New query added:" + query);
		}

		return queryID;
	}
}
