/**
 * @author pjohnso3@gmail.com
 */
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class Driver implements Runnable {

    private final String DEFAULT_TIMESTAMP_FORMAT = "HH.mm.ss.SS";
	private int total_thread_count;
	private int thread_num;
	private String user;
	private String port;
	private String host;
	private String password;
	private String database;
	private String statementText;
	private volatile boolean initializing;
	private volatile double tps;
	private volatile double statmentsRun;
	private volatile int status;
	private final int STOPPED = 0;
	private final int STARTED = 3;
	private final int MILLIS_TO_SECONDS = 1000;
	private boolean enableWLB;
	private boolean autoCommit;
	private boolean errorFirstEncountered = true;
	private Format timestampFormatter;
	
	public Driver(String[] args) {
		timestampFormatter = new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT);
		total_thread_count = 0;
		initializing = false;
		user = port = host = password = database = statementText = null;
		thread_num = 1;
		tps = 0;
		statmentsRun = 0;
		status = STOPPED;
		enableWLB = autoCommit = true;
		
		try {
			user = args[0];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Incorrect parameter '"+args[0]+"'");
			System.out.println("Usage: Driver <user> <port> <host> <password> <database> [number of threads] [enableWLB] [statment text file] [autocommit]");
			e.printStackTrace();
		}
		try {
			port = args[1];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Incorrect parameter '"+args[1]+"'");
			System.out.println("Usage: Driver <user> <port> <host> <password> <database> [number of threads] [enableWLB] [statment text file] [autocommit]");
			e.printStackTrace();
		}
		
		try {
			host = args[2];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Incorrect parameter '"+args[2]+"'");
			System.out.println("Usage: Driver <user> <port> <host> <password> <database> [number of threads] [enableWLB] [statment text file] [autocommit]");
			e.printStackTrace();
		}

		try {
			password = args[3];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Incorrect parameter '"+args[3]+"'");
			System.out.println("Usage: Driver <user> <port> <host> <password> <database> [number of threads] [enableWLB] [statment text file] [autocommit]");
			e.printStackTrace();
		}
		
		try {
			database = args[4];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Incorrect parameter '"+args[4]+"'");
			System.out.println("Usage: Driver <user> <port> <host> <password> <database> [number of threads] [enableWLB] [statment text file] [autocommit]");
			e.printStackTrace();
		}
		
		thread_num = 1;
		if (args.length > 5) {
			try {
				thread_num = Integer.parseInt(args[5]);
			} catch (NumberFormatException e) {
				System.out.println("Thread number NAN");
				e.printStackTrace();
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}

		if (args.length > 6) {
			try {
				String enableWLB_string = args[6];
				if (enableWLB_string.equalsIgnoreCase("false")) {
					enableWLB = false;
				} else if (enableWLB_string.equalsIgnoreCase("true")) {
					enableWLB = true;
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}

		if (args.length > 7) {
			try {
				String filename = args[7];
				statementText = fileToString("./"+filename);
				
			} catch (ArrayIndexOutOfBoundsException e) {
				statementText = null;
				e.printStackTrace();
			} catch (IOException e) {
				statementText = null;
				e.printStackTrace();
			}
		}

		if (args.length > 8) {
			try {
				String autoCommit_string = args[8];
				if (autoCommit_string.equalsIgnoreCase("false")) {
					autoCommit = false;
				} else if (autoCommit_string.equalsIgnoreCase("true")) {
					autoCommit = true;
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}

	}
	
    protected static String fileToString(final String filename) throws IOException {
    	StringBuilder file_string = new StringBuilder();
        
        try {
          BufferedReader input =  new BufferedReader(new FileReader(filename));
          try {
            String current_line = null;
            while (( current_line = input.readLine()) != null){
            	file_string.append(current_line);
            	file_string.append(System.getProperty("line.separator"));
            }
          }
          finally {
            input.close();
          }
        }
        catch (IOException ex){
        	throw ex;
        }
        return file_string.toString();
    }

	
	public void startWorkload() {
		initializing = true;
		status = STARTED;
		Thread tps_thread = new Thread(this);
		tps_thread.setName("tps_thread");
		tps_thread.start();

		for (int i=0; i<thread_num; i++) {
			Thread connection_thread = new Thread(this);
			connection_thread.setName("connection_thread_" + i);
			connection_thread.start();
			System.out.println("creating new thread " + connection_thread.getName() + ", thread number="+i);
			total_thread_count++;
		}
		initializing = false;
		System.out.println(thread_num + " threads created");
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
        	public void run() {
        		System.out.println("Shutting down");
        		status=STOPPED;
            }
        });

	}
	
	public static void main(String[] args) {
		Driver tester = new Driver(args);
		tester.startWorkload();
	}
	
	private void startCalculatingTPS() {
		while (initializing) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		double sleep_time = 1;
		double delta_time;
		double delta_transactionCount;
		double prev_transactionCount;
		double total_transactionCount = 0;
		double interval_start_time;
		double tps;
		while (status==STARTED) {
			interval_start_time = System.currentTimeMillis();
			
			try {
				Thread.sleep((long)sleep_time * MILLIS_TO_SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			delta_time = (System.currentTimeMillis() - interval_start_time) / MILLIS_TO_SECONDS;
			
			prev_transactionCount = total_transactionCount;
			total_transactionCount = statmentsRun;
			delta_transactionCount = total_transactionCount - prev_transactionCount;
			String currentTime = this.timestampFormatter.format(System.currentTimeMillis());
			tps = delta_transactionCount / delta_time;
			System.out.println("["+currentTime+"] -- tps="+tps);
		}
	}

	@Override
	public void run() {
		if (Thread.currentThread().getName().equalsIgnoreCase("tps_thread")) {
			startCalculatingTPS();
		} else {
			while (initializing) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// create connection and create/prepare statement
			Properties connection_properties = null;
			String db_url = "";
			Connection new_connection = null;
			try {
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				connection_properties = new Properties();
				connection_properties.put("user", user);
				connection_properties.put("password", password);
				if (enableWLB) {
					connection_properties.put("enableSysplexWLB", "true");
				}

				db_url = "jdbc:db2://"+host+":"+port+"/"+database;				
				new_connection = DriverManager.getConnection(db_url, connection_properties);
				new_connection.setAutoCommit(autoCommit);

				System.out.println("connection url = " + db_url);
				System.out.println("user='" + user + "' :: password='" + password + "'");
				System.out.println("Connection established for thread " + Thread.currentThread().getName());
	
			} catch (ClassNotFoundException e) {
				System.out.println("Driver not found");
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println("Connection not established");
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}			
			
			String query = "select firstnme from emp where firstnme = 'DIANNE'";
			if (this.statementText!=null && !this.statementText.equals("")) {
				query = statementText;
			}
			
			PreparedStatement pstmt = null;
			try {
				pstmt = new_connection.prepareStatement(query);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			
			boolean statement_result = false;
			double update_count = 0;
			status = STARTED;
			
			//execute statement until stopped
			while (status!=STOPPED) {
				try {
					pstmt.clearParameters();
					statement_result = pstmt.execute();
					
					SQLWarning sql_warning = pstmt.getWarnings();
					if (sql_warning != null) {
						String condition = sql_warning.getMessage();
						SQLWarning currentWarning = sql_warning;
						while(currentWarning!=null) {
							System.out.println("Warning: " + condition);
					    	sql_warning = sql_warning.getNextWarning();
						}
						
					}
					
					// first result is a ResultSet  object
					if (statement_result) {
						// statement succeeded, resultset object returned
						errorFirstEncountered = true;
						synchronized(this) {
							statmentsRun++;
						}
					} else {
						update_count = pstmt.getUpdateCount();
		            	if (update_count >= 0) {
		            		// statement succeeded, update count returned
							errorFirstEncountered = true;
							synchronized(this) {
								statmentsRun++;
							}
		        		} else if (update_count==-1) {
		        			// statement failed
							System.out.println("'" + query + "' not successful for thread " + Thread.currentThread().getName());
		            	}
					}
						
				} catch (SQLException e) {
					if (errorFirstEncountered) {
						System.out.println("Statement not run");
						e.printStackTrace();
						errorFirstEncountered = false;
					}
				} catch (NullPointerException e) {
					if (errorFirstEncountered) {
						System.out.println("Connection is null");
						e.printStackTrace();
						errorFirstEncountered = false;
					}
				}
			}

			try {
				pstmt.clearWarnings();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				System.out.println("Statement is null");
				e.printStackTrace();
			}

			try {
				pstmt.clearParameters();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				System.out.println("Statement is null");
				e.printStackTrace();
			}

			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				System.out.println("Statement is null");
				e.printStackTrace();
			}

			try {
				System.out.println("closing connections");
				new_connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				System.out.println("Statement is null");
				e.printStackTrace();
			}
		}
	}
}
