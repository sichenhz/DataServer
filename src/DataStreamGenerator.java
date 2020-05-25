import java.net.*;
import java.io.*;

/* ---------------------------------------------------------------- */
/* ---------------------------Completed---------------------------- */
/* ---------------------------------------------------------------- */
public class DataStreamGenerator {

	public static void main(String[] args) throws Exception {
		
		System.out.println("Data Stream Generator Started.");
		
		try {
			
			ServerSocket ss = new ServerSocket(9099);
			Socket s = ss.accept();
			
			System.out.println("Socket connection succeed.");

	    	DataOutputStream out = new DataOutputStream(s.getOutputStream());  

			String txtFile = "/home/ubuntu/upload/Tweets.txt";
			String line = "";
			int counter = 0;
			long timeInterval = 1L;
			
			try (BufferedReader br = new BufferedReader(new FileReader(txtFile))) {
				
				while ((line = br.readLine()) != null) {
					if (counter > 0) {
						out.writeUTF(line);
					}
					counter++;
					Thread.sleep(1000 * timeInterval);
					out.flush();
				}

				s.close();
				ss.close();

			} catch (Exception e) {
	            e.printStackTrace();
			}			
		} catch (NullPointerException e) {
			System.out.println("Exception: Client Disconnected");
		} catch (IOException e) {
			System.out.println("Exception: An I/O error occurs when opening the socket or waiting for a connection");
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
