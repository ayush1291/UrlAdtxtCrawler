package adpushup.jdbc;

import adpushup.ConfigProperties;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * A Jdbc Connector class to provide connection for configured db and some helper methods to run queries.
 * @author Ayush
 */
public class JdbcConnector {

    /**
     * Establishes connection with sql database as per given configurations
     * @return Connection formed, or else null
     */
    public static Connection getConnection(){
        Connection con=null;
        try{
            Class.forName(ConfigProperties.getProperties().get("jdbc.driver"));
            con = DriverManager.getConnection(ConfigProperties.getProperties().get("jdbc.url"),getPropertiesForJdbcConnection());
        }catch (Exception e){
            e.printStackTrace();
        }
        return con;
    }

    /**
     * Fetches configured properties for jdbc connection
     * @return Properties object containing all the properties for jdbc connection
     */
    private static Properties getPropertiesForJdbcConnection(){
        final Properties jdbcProps = new Properties();
        ConfigProperties.getProperties().entrySet().stream().filter(k->k.getKey().contains("jdbc.property")).forEach((e)->{
            jdbcProps.put(e.getKey().substring(13,e.getKey().length()),e.getValue());
        });
        return jdbcProps;
    }

    public static void closeConnection(Connection con){
        try {
            con.close();
        } catch (SQLException throwables) {
        }finally {
            try{
                if(!con.isClosed()) con.close();
            }catch (SQLException e){}
        }
    }

    /**
     *Executes update and delete queries
     * @param query
     * @param conn
     * @return int
     */
    public static int executeUpdateQuery(String query, Connection conn){
        int x=0;
        try {
            Statement stmt = conn.createStatement();
            x = stmt.executeUpdate(query);
        } catch (SQLException e) {
        }
        return x;
    }

    /**
     *Executes update and delete named queries
     * @param query
     * @param conn
     * @return int
     */
    public static int executeUpdateQuery(String query, Connection conn, Map<String,Object> mapper){
        int x=0;
        try {
            for(Map.Entry<String,Object> e:mapper.entrySet()){
                if(query.contains(":"+e.getKey())){
                    query=query.replace(":"+e.getKey(),addToQuery(e.getValue()));
                }
            }
            Statement stmt = conn.createStatement();
            x = stmt.executeUpdate(query);
        } catch (SQLException e) {
        }
        return x;
    }

    /**
     * Executes select query
     * @param query
     * @param conn
     * @return ResultSet containing results for the query executed
     */
    public static ResultSet executeQuery(String query, Connection conn){
        ResultSet res=null;
        try {
            Statement stmt = conn.createStatement();
             res = stmt.executeQuery(query);
        } catch (SQLException e) {
        }
        return res;
    }

    /**
     * Executes select named query
     * @param query
     * @param conn
     * @return ResultSet containing results for the query executed
     */
    public static ResultSet executeQuery(String query, Connection conn, Map<String,Object> mapper){
        ResultSet res=null;
        try {
            for(Map.Entry<String,Object> e:mapper.entrySet()){
                if(query.contains(":"+e.getKey())){
                    query=query.replace(":"+e.getKey(),addToQuery(e.getValue()));
                }
            }
            Statement stmt = conn.createStatement();
            res = stmt.executeQuery(query);
        } catch (SQLException e) {
        }
        return res;
    }

    /**
     * Helper method to add to query for insert queries
     * @param newVal
     * @return string value for adding into insert query
     */
    private static String addToQuery(Object newVal){
        if(newVal instanceof String) return "'"+newVal.toString()+"'";
        return newVal.toString();
    }
}
