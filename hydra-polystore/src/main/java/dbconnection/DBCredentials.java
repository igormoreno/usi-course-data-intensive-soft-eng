package dbconnection;

import java.util.HashMap;
import java.util.Map;


public class DBCredentials {
    private String dbName;
    private String url;
    private int port;
    private String userName;
    private String userPwd;
    private String dbType;
	private static Map<String, DBCredentials> dbPorts = new HashMap<String, DBCredentials>();

    protected DBCredentials(String dbName, String url, int port, String userName, String userPwd, String dbType) {
        this.dbName = dbName;
        this.url = url;
        this.port = port;
        this.userName = userName;
        this.userPwd = userPwd;
        this.dbType = dbType;
    }
	
	static {
			dbPorts.put("reldata", new DBCredentials("reldata", "hydra.unamurcs.be", 33063, "root", "password","mysql"));
			dbPorts.put("myMongoDB", new DBCredentials("", "hydra.unamurcs.be", 27013, "", "","mongodb"));
			dbPorts.put("myRedis", new DBCredentials("", "hydra.unamurcs.be", 63793, "", "","redis"));
	}

	public static Map<String, DBCredentials> getDbPorts() {
        return dbPorts;
    }


    public String getDbName() {
        return dbName;
    }

    public String getUrl() {
        return url;
    }

    public int getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserPwd() {
        return userPwd;
    }

    public String getDbType() {
        return dbType;
    }

    public String getDbTypeForJDBCConnection() {
	    switch (dbType) {
            case "mysql":
            case "mariadb":
                return "mysql";
            default:
                return dbType;
        }
    }

}

