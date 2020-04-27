import java.net.*;
import java.io.*;
import java.util.*;

public class Worker {

	public static void receiveTweets(Socket s) {
		try {
			// Instantiate input and output streams
			DataInputStream in = new DataInputStream(s.getInputStream());

			ArrayList<HashMap<String, String>> tweets = new ArrayList<>();
			// Infinite loop to establish communications
			while (true) {
				// Read a tweet string from the server
				String str = in.readUTF();
				System.out.println("Tweet saved: " + str);
				String[] tweet = str.split("\t");

				Map<String, String> tweetMap = new HashMap<String, String>();
				tweetMap.put("tweet_id", tweet[0]);
				tweetMap.put("airline_sentiment", tweet[1]);
				tweetMap.put("airline", tweet[2]);
				tweetMap.put("text", tweet[3]);
				tweetMap.put("tweet_created", tweet[4]);
				tweets.add((HashMap<String, String>) tweetMap);
			}
		} catch (NullPointerException e) {
			System.out.println("Exception: Client Disconnected");
		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void handleQuery(Socket s) {
		try {
			// Instantiate input and output streams
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());

			// Infinite loop to establish communications
			while (true) {
				// Read a query string from the server
				String queryString = in.readUTF();
				String[] queryArray = queryString.split("\t");
				String type = queryArray[0];
				String text = queryArray[1];

				out.writeUTF(executeQuery(type, text));
			}
		} catch (NullPointerException e) {
			System.out.println("Exception: Client Disconnected");
		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static void main(String[] args) throws Exception {

		System.out.println("Worker Started ....");

		Socket s_database = new Socket(InetAddress.getLocalHost(), 9001);
		Socket s_handle = new Socket(InetAddress.getLocalHost(), 9002);

		new Thread(new Runnable() {
			public void run() {
				receiveTweets(s_database);
			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				handleQuery(s_handle);
			}
		}).start();
	}
	
	public static String executeQuery(String type, String text) {
		/* ---------------------------------TODO 3--------------------------------------- */
		/* --- Handle 4 types of operations to get the result --------------------------- */
		/* --- type 1 means to search a text by a tweet ID ------------------------------ */
		/* --- type 2 means search a number of tweets containing a specific words ------- */
		/* --- type 3 means search a number of tweets from a specific airline ----------- */
		/* --- type 4 means find the most frequent character in a tweet by a tweet ID --- */
		return text + " Time consuming: xxx seconds";
	}
}
