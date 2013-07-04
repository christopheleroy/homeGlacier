package net.cleroy.glacier.sqllite;


import java.sql.*;
import java.util.HashSet;
import java.util.List;

import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;

public class InitialDb {
	
	static public void main(String[] args) throws Exception {
		if(args.length == 0 ||
				(! "freshen".equals(args[0]) &&
				(args.length != 2 || !"build".equals(args[0])))) {
			System.err.println("usage: initialdb freshen|build <file>\nFreshen: presumes access to the ArchConfig.properties and freshens the list of partitions in the database.\nbuild <file> builds an empty database\n");
		}else if(args.length == 2 && "build".equals(args[0])) {
			buildDb(args[1]);
		}else if(args.length==1 && "freshen".equals(args[0])) {
			ArchConfig cfg = new ArchConfig();
			freshenPartition(cfg);
		}
		
	}
	
	
	static private void freshenPartition(ArchConfig cfg) throws Exception {
		Connection c = null;
		List<PartitionConfig> parts = cfg.getPartitionConfigs();
		HashSet<Integer> partsInDB = new HashSet<Integer>();
		
		c = cfg.makeConnection();
		String sql = "SELECT ID FROM PARTITION";
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) { partsInDB.add(new Integer(rs.getInt(1))); }
		
		String sqlInsert = "INSERT into PARTITION(ID, HOSTNAME, ROOT, ENCRYPT)values(?, 'tbd', 'tbd', 1)";
		String sqlUpdate = "UPDATE PARTITION SET HOSTNAME = ?, ROOT = ? WHERE ID = ?";
		PreparedStatement  psInsert = c.prepareStatement(sqlInsert);
		PreparedStatement psUpdate = c.prepareStatement(sqlUpdate);
		for(PartitionConfig pcfg: parts) {
			if(!partsInDB.contains(new Integer(pcfg.getId()))) {
				psInsert.clearParameters();
				psInsert.setInt(1, pcfg.getId());
				psInsert.execute();
			}
			psUpdate.clearParameters();
			psUpdate.setInt(3, pcfg.getId());
			psUpdate.setString(1, pcfg.getHostname());
			psUpdate.setString(2, pcfg.getRoot());
			psUpdate.execute();
		}
		c.commit();
		
	}

	
	static private void buildDb(String file) {
		Connection c = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:" + file);
	      
	      Statement stmt = null;
	      
	      stmt = c.createStatement();
	      String[] sqls  =  {
	       "CREATE TABLE PARTITION(" + 
	       "ID INT PRIMARY KEY NOT NULL, " +
	       "HOSTNAME CHAR(200) NOT NULL, " +
	       "ROOT CHAR(200) NOT NULL," +
	       "ENCRYPT INT NOT NULL DEFAULT 0)",
	       
	       "CREATE TABLE PATHNAME ( " + 
	       	"ID INTEGER PRIMARY KEY NOT NULL, " + 
	       	"PARTITION_ID INT NOT NULL, " +
	       	"PATH TEXT NOT NULL, " +
	       	"UNIQUE(PARTITION_ID, PATH), " +
	       	"FOREIGN KEY(PARTITION_ID) REFERENCES PARTITION(ID) )",
	        
	       "CREATE TABLE ARCHIVE(" +
	      	"ID INTEGER PRIMARY KEY NOT NULL, " + 
	      	"PARTITION_ID INT, " + 
	      	"SHA256 CHAR(64)," +
	      	"SALTED CHAR(64)," +
	      	"VAULT_NAME CHAR(200), " + 
	      	"GLACIER_ID CHAR(200), " + 
	      	"FOREIGN KEY(PARTITION_ID) REFERENCES PARTITION(ID) )", 

	      "CREATE TABLE ARCHIVE_CONTENT(" + 
	      	"ARCHIVE_ID INT NOT NULL, " +
	      	"PATH_ID INT NOT NULL," +
	      	"FILENAME TEXT NOT NULL, " +
	      	"TIMEST INTEGER NOT NULL, " +
	      	"FILE_SIZE INT NOT NULL, " + 
	      	"SHA256 CHAR(64), " +
	      	"FOREIGN KEY(PATH_ID) REFERENCES PATHNAME(ID), " +
	      	"FOREIGN KEY(ARCHIVE_ID) REFERENCES ARCHIVE(ID) )",
	      	
	      "CREATE INDEX PATHINX ON PATHNAME(PARTITION_ID, PATH)",
	      "CREATE INDEX FILEINX ON ARCHIVE_CONTENT(PATH_ID, FILENAME)",
	      
	       };
	      
	      for(String sql : sqls) {
	    	  stmt = c.createStatement();
	    	  stmt.executeUpdate(sql);
	    	  System.out.println("Executed: " + sql.substring(0,30) + " ... ");
	      }
	    
	      
	    } catch ( Exception e ) {
	      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      System.exit(0);
	    }
	    System.out.println("Opened database successfully");
	}

}
