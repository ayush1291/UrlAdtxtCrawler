package adpushup.dbrelation;

import adpushup.jdbc.JdbcConnector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A Db relation mapped to advertiser table.
 */
public class Advertiser {
    private static final String TABLE_NAME="advertiser";
    private static final String SQL_FIND_ALL="select * from "+TABLE_NAME;

    private static int curNo=0;
    private static HashMap<String,Integer> map = null;

    private Integer id;
    private String name;

    private Advertiser(){}

    private Advertiser(int id, String name){
        this.id=id; this.name=name;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * Gets the id mapped with advertiser name
     * @param name
     * @return Integer
     */
    public static Integer getAdvertiser(String name){
        if(map==null) initIncrementer();
        if(map.containsKey(name)) return map.get(name);
        synchronized (Advertiser.class){
            if(map.containsKey(name)) return map.get(name);
            map.put(name,++curNo);
            insert(new Advertiser(curNo,name));
        }
        return map.get(name);
    }

    /**
     * Initialise the incrementer for storing different advertisers
     */
    private static void initIncrementer(){
        if(map!=null) return;
        synchronized (Advertiser.class){
            if(map==null){
                map = new HashMap<>();
                List<Advertiser> list =findAll();
                for (Advertiser advertiser : list) {
                    map.put(advertiser.name,advertiser.id);
                }
                curNo=list.size();
            }
        }
    }

    /**
     * Inserts a record into advertiser table
     * @param advertiser
     * @return boolean
     */
    public static boolean insert(Advertiser advertiser){
        try(Connection con = JdbcConnector.getConnection()){
            int x=JdbcConnector.executeUpdateQuery(AdvertiserHelper.createQueryToInsert(advertiser),con);
            return x>0;
        } catch (SQLException e) {
        }
        return false;
    }

    /**
     * Get all records from the table
     * @return List<Advertiser>
     */
    public static List<Advertiser> findAll(){
        List<Advertiser> res=null;
        try(Connection con = JdbcConnector.getConnection()){
             res= AdvertiserHelper.mapColumns(JdbcConnector.executeQuery(SQL_FIND_ALL,con));
        } catch (SQLException e) {
        }
        return res;
    }

    /**
     * A helper class for db opertions on the advertiser table
     */
    private static class AdvertiserHelper {

        /**
         * Maps all the rows returned into a list of Advertiser class
         * @param resultSet
         * @return List
         * @throws SQLException
         */
        public static List<Advertiser> mapColumns(ResultSet resultSet) throws SQLException {
            List<Advertiser> res = new ArrayList<>();
            while(resultSet.next()){
                Advertiser advertiser= new Advertiser();
                advertiser.id= resultSet.getInt(1);
                advertiser.name = resultSet.getString(2);
                res.add(advertiser);
            }
            return res;
        }

        /**
         * Creates an insert query from the given object
         * @param advertiser
         * @return String
         */
        public static String createQueryToInsert(Advertiser advertiser){
            StringBuilder query=new StringBuilder();
            query.append("Insert into "+TABLE_NAME+" (id,name) values ("+advertiser.getId()+",'"+advertiser.getName()+"'"+")");
            return query.toString();
        }

    }
}
