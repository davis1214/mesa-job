package azkaban.utils;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * Created by Administrator on 16/7/22.
 */
public class JdbcUtil {

    private Vector<Connection> pool;
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private int poolSize = 1;
    private static JdbcUtil instance = null;

    private JdbcUtil(String jdbcUrl) {
        init( jdbcUrl);
    }

    private void init(String jdbcUrl) {
        readConfig(jdbcUrl);
        pool = new Vector<Connection>(poolSize);
        addConnection();
    }

    public synchronized void release(Connection coon) {
        pool.add(coon);
    }

    public synchronized void closePool() {
        for (int i = 0; i < pool.size(); i++) {
            try {
                ((Connection) pool.get(i)).close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            pool.remove(i);
        }
    }

    public static JdbcUtil getInstance(String jdbcUrl) {
        if (instance == null) {
            instance = new JdbcUtil(jdbcUrl);
        }
        return instance;
    }

    public synchronized Connection getConnection() {
        if (pool.size() > 0) {
            Connection conn = pool.get(0);
            pool.remove(conn);
            return conn;
        } else {
            return null;
        }
    }

    private void addConnection() {
        Connection coon = null;
        for (int i = 0; i < poolSize; i++) {
            try {
                Class.forName(driverClassName);
                coon = java.sql.DriverManager.getConnection(url, username, password);
                pool.add(coon);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void readConfig(String jdbcUrl) {
        try {
            String[] splits = jdbcUrl.split("\\?");
            String url = splits[0];

            String[] userAndPasswd = splits[1].split("&");
            Map<String,String> map = new HashMap<String,String>();

            map.put("url", url);
            for (String kv : userAndPasswd) {
                String[] kvs = kv.split("=");
                map.put(kvs[0], kvs[1]);
            }

            this.driverClassName = map.get("driverClassName");
            if(this.driverClassName == null || this.driverClassName.length() == 0){
                this.driverClassName = "com.mysql.jdbc.Driver";
            }
            this.username = map.get("user");
            this.password = map.get("password");
            this.url = map.get("url");
            String poolSize = map.get("poolsize");
            if(poolSize==null || poolSize.length() ==0){
                poolSize = "10";
            }
            this.poolSize = Integer.parseInt(poolSize);
        }  catch (Exception e) {
            e.printStackTrace();
        }
    }

}