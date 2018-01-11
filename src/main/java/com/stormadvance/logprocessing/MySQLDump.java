package com.stormadvance.logprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.storm.tuple.Tuple;
/**
 * This class contains logic to persist record into MySQL database.
 * 
 */
public class MySQLDump {
	/**
	 * Name of database you want to connect
	 */
	private String database;
	/**
	 * Name of MySQL user
	 */
	private String user;
	/**
	 * IP of MySQL server
	 */
	private String ip;
	/**
	 * Password of MySQL server
	 */
	private String password;
	
	public MySQLDump(String ip, String database, String user, String password) {
		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	/**
	 * Get the MySQL connection
	 */
	private Connection connect = MySQLConnection.getMySQLConnection(ip,database,user,password);

	private PreparedStatement preparedStatement = null;
	
	/**
	 * Persist input tuple.
	 * @param tuple
	 */
	public void persistRecord(Tuple tuple) {
		try {

			// preparedStatements can use variables and are more efficient
			String query = "INSERT INTO `testing` (`c1`, `c2`, `c3`) VALUES ('c1', 'c2', 'c3')";
			preparedStatement = connect.prepareStatement(query);

			//preparedStatement.setString(1, tuple.getStringByField("c1"));
			//preparedStatement.setString(2, tuple.getStringByField("c2"));
			//preparedStatement.setString(3, tuple.getStringByField("c3"));

			
			// Insert record
			preparedStatement.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// close prepared statement
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (Exception exception) {
					System.out
							.println("Error occure while closing PreparedStatement : ");
				}
			}
		}

	}
	
	public void close() {
		try {
		connect.close();
		}catch(Exception exception) {
			System.out.println("Error occure while clossing the connection");
		}
	}
	
	
}
