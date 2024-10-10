package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.SQLException;

public class JdbcCreateAndCallStoredProcedure {

    // JDBC URL, username, and password
    private static final String URL = "jdbc:mysql://localhost:3306/procedureDB";
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    // Define constants for sizes
    private static final int PAGE_SIZE = 8192; // 8KB
    private static final int BLOCK_SIZE = 4096; // 4KB
    private static final int TOTAL_RECORDS = 1000000; // Total records to insert

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        CallableStatement callableStatement = null;

        try {
            // Load MySQL JDBC Driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Establish connection
            connection = DriverManager.getConnection(URL, USER, PASSWORD);

            // 1. Drop the existing stored procedure if it exists
            String dropProcedureSQL = "DROP PROCEDURE IF EXISTS INSERT_MILLION_RECORDS";
            statement = connection.createStatement();
            statement.executeUpdate(dropProcedureSQL);
            System.out.println("Stored procedure dropped if it existed.");

            // 2. Create the stored procedure (no DELIMITER needed)
            String createProcedureSQL =
                    "CREATE PROCEDURE INSERT_MILLION_RECORDS() " +
                            "BEGIN " +
                            "   DECLARE i INT DEFAULT 0; " +
                            "   WHILE i < 1000000 DO " +
                            "      INSERT INTO large_table(name, value) " +
                            "      VALUES (CONCAT('Name', i), FLOOR(1 + RAND() * 1000000)); " +
                            "      SET i = i + 1; " +
                            "   END WHILE; " +
                            "END";

            // Execute the SQL to create the procedure
            statement.executeUpdate(createProcedureSQL);
            System.out.println("Stored procedure created successfully!");

            // 3. Call the stored procedure to insert 1 million records
            String callProcedureSQL = "{CALL INSERT_MILLION_RECORDS()}";
            callableStatement = connection.prepareCall(callProcedureSQL);

            System.out.println("Inserting 1 million records, please wait...");
            callableStatement.execute();
            System.out.println("1 million records inserted successfully!");

            // 4. Calculate total data size and blocks read
            int totalDataSize = (TOTAL_RECORDS * PAGE_SIZE) / 1000000; // Size based on number of records, assuming 8KB per record
            int totalBlocksSize = (totalDataSize + BLOCK_SIZE - 1) / BLOCK_SIZE; // Round up to the nearest block

            // Output total data size and block reads
            System.out.println("Total Data Size: " + totalDataSize + " bytes");
            System.out.println("Total Blocks Read: " + totalBlocksSize + " blocks (4KB each)");

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (callableStatement != null) callableStatement.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
