import java.net.*;
import java.io.*;
import java.util.*;

/* ---------------------------------TODO in this page--------------------------------------- */
/* --- 1. Connect all work utility function to server --------------------------- */
/* --- 2. Write the processing time function 		------------------------------ */
/* --- type 2 means search a number of tweets containing a specific words ------- */

public class Worker {
	
	static ArrayList<HashMap<String, String>> tweets = new ArrayList<>();
	
	public static void receiveTweets(Socket s) {
		
		try {
			// Instantiate input and output streams
			DataInputStream in = new DataInputStream(s.getInputStream());
				
			
			
			// Infinite loop to establish communications
			while (true) {
				
				//Read a tweet string from the server
				String str = in.readUTF();
//				System.out.println("Tweet saved: " + str);
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
			//Instantiate input and output streams
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
		/* ---------------------------------TODO Utility function complete--------------------------------------- */
		/* --- Handle 4 types of operations to get the result --------------------------- */
		/* --- type 1 means to search a text by a tweet ID(Done) ------------------------------ */
		/* --- type 2 means search a number of tweets containing a specific words ------- */
		/* --- type 3 means search a number of tweets from a specific airline ----------- */
		/* --- type 4 means find the most frequent character in a tweet by a tweet ID(Done) --- */
		if(type.equals("1"))
		{
			//timer starts
			searchTextByID(text);
//			//timer ends
//			return "It takes " + " time " ;
			//return searchTextByID(text);
		}else if(type.equals("2"))
		{
			return searchTweetByWord(text);
		}else if(type.equals("3"))
		{
			return searchTweetByAirline(text);
		}else if(type.equals("4"))
		{
			return findMostFrequentChar(text) + " Time consuming: xxx seconds";
		} 
	
		return "nothing found";
			
	}
	
	/** 
	 * 
	 * @param tweetID
	 * @return Tweet text correspond to the tweeetID. Otherwise Data doesn't exist
	 * Test passed
	 */
	private static String searchTextByID(String tweetID) {
		//start a timer
		synchronized(tweets)
		{
			if (tweets.size() != 0) {

				for (int i = 0; i < tweets.size(); i++) {

					Map<String, String> map = tweets.get(i);

					if (map.get("tweet_id").equals(tweetID)) {
						//end a timer 
						return map.get("text");
					}
				}

			}
			return "No tweets match ID";
		}
		
	}
	
	/**
	 * @param tweetID
	 * @return Most frequent character in a text.
	 */
	private static String findMostFrequentChar(String tweetID)
	{
		synchronized (tweets) 
		{
			if (tweets.size() != 0) {

				for (int i = 0; i < tweets.size(); i++) {

					Map<String, String> map = tweets.get(i);
					System.out.println("String is" + map.get("text"));
					if (map.get("tweet_id").equals(tweetID)) {
						return String.valueOf(freqCharFinder(map.get("text")));
					}
				}

			}
			return "Tweet doesn't exist";
		}
		
	}
	
	/**
	 * @param string as the tweet text
	 * @return Find the most Frequent Character
	 */
	private static char freqCharFinder(String string) {
		final int ASCII_SIZE = 256;
		String cleanString = " ";

		// replace all non-alphabetic chars from a String and store it into a new string
		for (int i = 0; i < string.length(); i++) {
			cleanString = string.replaceAll("[^a-zA-Z]", "").toLowerCase();
		}
			
		int count[] = new int[ASCII_SIZE];
		int length = cleanString.length();

		int max = -1; // Initialize max count
		char result = ' '; // Initialize result

		
		// Construct character count array from the input string
		for (int i = 0; i < length; i++)
		{
			count[cleanString.charAt(i)]++;
		}
			
		
		// Traverse through the string and maintaining the count of each char
		for (int i = 0; i < length; i++) 
		{
			if (count[string.charAt(i)]> max) 
			{
				max = count[cleanString.charAt(i)];
				result = cleanString.charAt(i);
			}
		}
		return result;
	}
	
	/**
	 * 
	 * @param tweet
	 * find how man tweets containing a specific word
	 */
	private static String searchTweetByWord(String tweetWord)
	{
		System.out.println("tweetWord is " + tweetWord);
		synchronized (tweets) 
		{
			int count = 0;
			if (tweets.size() != 0) {

				for (int i = 0; i < tweets.size(); i++) {

					Map<String, String> map = tweets.get(i);

					if (map.get("text").toLowerCase().contains(tweetWord.toLowerCase())) {
						count++;
					}

					return "There are " + String.valueOf(count) + " containing " + tweetWord;
				}

			}

			return tweetWord + " doesn't exist in any tweets";
		}
		
	}
	
	/**
	 * @param airlineName
	 * search a number of tweets from a specific airline
	 */
	private static String searchTweetByAirline(String airlineName)
	{
		System.out.println("airlinename is " + airlineName);
		synchronized (tweets) 
		{
			int count = 0;

			if (tweets.size() != 0) {

				for (int i = 0; i < tweets.size(); i++) {

					Map<String, String> map = tweets.get(i);

					if (map.get("airline").toLowerCase().contains(airlineName.toLowerCase())) {
						count++;
					}
				}
				return "There are " + String.valueOf(count) + " containing " + airlineName;
			}

			return airlineName + "doesn't exist in any tweets";
		}

	}
		

	
}
