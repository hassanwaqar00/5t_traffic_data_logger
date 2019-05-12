package it.polito.5t.trafficdatalogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBHelper {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://DB_IP_HERE:DB_PORT_HERE/DB_NAME_HERE?zeroDateTimeBehavior=convertToNull";
    static final String DB_USERNAME = "DB_USER_NAME_HERE";
    static final String DB_PASSWORD = "DB_PASSWORD_HERE";
    private Connection connection = null;

    public DBHelper() {
        jdbcConnect();
    }

    public Connection jdbcConnect() {
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(DBHelper.class.getName() + ".jdbcConnect").log(Level.WARNING, ex.toString());
            connection = null;
        }
        return connection;
    }

    public void jdbcClose() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {
                Logger.getLogger(DBHelper.class.getName() + ".jdbcClose").log(Level.WARNING, ex.toString());
            }
            connection = null;
        }
    }

    public void insertTrafficData(String start_time, String end_time, int lcd1, int Road_LCD, String Road_name, int offset, String direction, double lat, double lng, int accuracy, int period, double flow, double speed) throws SQLException {
        String query = "INSERT IGNORE INTO `traffic_data`"
                + "(`start_time`, `end_time`, `lcd1`, `Road_LCD`, `Road_name`, `offset`, `direction`, `lat`, `lng`, `accuracy`, `period`, `flow`, `speed`) "
                + "VALUES "
                + "('" + start_time + "','" + end_time + "','" + lcd1 + "','" + Road_LCD + "',\"" + Road_name + "\",'" + offset + "','" + direction + "',ROUND('" + lat + "',4),ROUND('" + lng + "',4),'" + accuracy + "','" + period + "','" + flow + "',ROUND('" + speed + "',2))";
        Statement statement;
        statement = connection.createStatement();
        statement.execute(query);
        statement.close();
        statement = null;
    }
}
