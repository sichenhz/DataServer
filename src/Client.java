import java.net.*;
import java.io.*;
import java.util.*;

public class Client {

	public static void main(String[] args) throws Exception {

		try {
			// Instantiate a scanner
			Scanner scanner = new Scanner(System.in);

			// Instantiate a socket
			Socket s = new Socket(InetAddress.getLocalHost(), 9098);

			// Instantiate input and output streams
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());

			// Infinite loop to establish communication between client and server
			while (true) {
				// Read a menu from the server
				// Welcome to DataServer, please type a number to access a service:
				// 1 to register a new password
				// 2 to access services via a password
				// 3 to exit
				for (int i = 0; i < 4; i++) {
					System.out.println(in.readUTF());
				}

				// Send an option to the server
				String option = scanner.nextLine();
				out.writeUTF(option);

				if (option.equalsIgnoreCase("1")) {
					// Read a password from the server
					System.out.println(in.readUTF());

				} else if (option.equalsIgnoreCase("2")) {
					// Read "Enter your password:" from the server
					System.out.println(in.readUTF());

					while (true) {
						// Send a password to the server
						out.writeUTF(scanner.nextLine());
						// Read "Connection Success" or "Invalid Password, please enter your password
						// again:" from the server
						String result = in.readUTF();
						System.out.println(result);

						if (result.equalsIgnoreCase("Connection Success")) {
							break;
						}
					}
					
					// Infinite loop to establish communication between client and server
					while (true) {
						// Read a menu from the server
						// Success to access your services, please type a number to access a service:
						// 1 to search a text by a tweet ID
						// 2 to search a number of tweets containing a specific words
						// 3 to search a number of tweets from a specific airline
						// 4 to find the most frequent character in a tweet by a tweet ID
						// 5 to get a status/result by a query ID
						// 6 to exit
						for (int i = 0; i < 7; i++) {
							System.out.println(in.readUTF());
						}

						// Send an option to the server
						option = scanner.nextLine();
						
						out.writeUTF(option);

						if (option.equalsIgnoreCase("1") || option.equalsIgnoreCase("2") || option.equalsIgnoreCase("3") || option.equalsIgnoreCase("4")) {
							System.out.println(in.readUTF());
							out.writeUTF(appendingDeadline(scanner.nextLine()));
							System.out.println(in.readUTF());
							
						} else if (option.equalsIgnoreCase("5")) {
							System.out.println(in.readUTF());
							out.writeUTF(scanner.nextLine());
							System.out.println(in.readUTF());
							
						} else if (option.equalsIgnoreCase("6")) {
							break;

						} else {
							System.out.println("INVALID OPTION!! PLEASE SPECIFY OPTION AGAIN.");
						}
					}
				} else if (option.equalsIgnoreCase("3")) {
					// Read "Exit Success" from the server and exit
					System.out.println(in.readUTF());
					break;

				} else {
					System.out.println("INVALID OPTION!! PLEASE SPECIFY OPTION AGAIN.");
				}
			}
			// Close the streams and the client
			in.close();
			out.close();
			s.close();

			// Close the scanner
			scanner.close();
		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static String appendingDeadline(String option) 
	{
		/* --------------------------------------TODO 1------------------------------------------ */
		/* --- 1. Randomly generate a deadline(time), there can also be no deadline ------------- */
		/* --- 2. If there is a deadline(time), the server will execute the query first --------- */
		/* --- 3. If there is a no deadline(time), the server will execute the query in order --- */
		/* --- 4. Send the option appending the deadline to the server -------------------------- */
		/* -------------------------------------------------------------------------------------- */
		Random rand = new Random();
		int deadline = rand.nextInt(10000); // 10 seconds in milliseconds

		// assuming that any deadline less than a second is regarded as ASAP
		if (deadline < 5000) {
			return option + "	" + deadline;
		} else {
			return option; // appends no deadline
		}
		
	}
}
