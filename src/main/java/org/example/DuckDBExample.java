package org.example;

import java.net.URISyntaxException;
import java.sql.*;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DuckDBExample {
    public static void main(String[] args) {
        try {
            // Load the JDBC Driver
            Class.forName("org.duckdb.DuckDBDriver");
            // Creat DB Connection
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            // if DB connection are successfully, send a mesagge
            System.out.println("DuckDB connection succesfully!");

            //Create a statement object for sending SQL statements to the database
            Statement stmt = conn.createStatement();
            //Executing the SQL Query
            ResultSet rs = stmt.executeQuery("SELECT current_date");
            //Processing the Results:
            Date currentDate = rs.next() ? rs.getDate(1) : null;


            System.out.println("Current Date: " + currentDate);


            // Get the path to the CSV file
            URL resource = DuckDBExample.class.getClassLoader().getResource("customer.csv");
            if (resource == null) {
                throw new IllegalArgumentException("file not found! " + "customer.csv");
            }

            // Convert the URL to a file path
            String csvPath = Paths.get(resource.toURI()).toString();

            // Create the customer table and populate it with data from the CSV file
            String createTableQuery = "CREATE TABLE customer AS SELECT * FROM read_csv_auto('" + csvPath + "')";
            stmt.execute(createTableQuery);

            // Verify that data was inserted by querying the table
            rs = stmt.executeQuery("SELECT * FROM customer");

            // Process and print the results
            while (rs.next()) {
                int customerId = rs.getInt("CustomerId");
                String firstName = rs.getString("FirstName");
                String lastName = rs.getString("LastName");
                String gender = rs.getString("Gender");
                System.out.println("CustomerId: " + customerId + ", FirstName: " + firstName + ", LastName: " + lastName + ", Gender: " + gender);
            }

            //SELECT FirstName, LastName, Gender FROM read_csv('customer.csv');


            // Close the result set, statement, and connection to release resources
            stmt.close();
            rs.close();
            conn.close();




        }catch (ClassNotFoundException | SQLException e) {
            // Print the stack trace if an exception occurs
            e.printStackTrace();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
