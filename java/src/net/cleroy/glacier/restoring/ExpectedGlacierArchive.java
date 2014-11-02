package net.cleroy.glacier.restoring;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import net.cleroy.glacier.archiving.Archive;

public class ExpectedGlacierArchive {
	
	protected Archive archive;
	protected String salted;
	
	
	/** load the GlacierArchives for a partition (@param configId)
	 * The GlacierArchive give a highlevel view of an archive, but not its actual content.
	 * @param c a connection to the db
	 * @param configId
	 * @return
	 * @throws SQLException
	 */
	public static List<ExpectedGlacierArchive> loadAllArchivesFromDb(Connection c, int configId) throws SQLException {
		List<ExpectedGlacierArchive> archs = new ArrayList<ExpectedGlacierArchive>();
		
		String sql = "SELECT a.Id, a.glacier_Id, a.sha256, a.salted, " +
		 " count(*), sum(m.file_size), min(m.timest), max(m.timest) " +
		 "FROM ARCHIVE a join ARCHIVE_CONTENT m ON (m.ARCHIVE_ID = a.ID) " +
		 "WHERE a.PARTITION_ID = " + configId +
		 " GROUP BY a.Id, a.glacier_Id, a.sha256, a.salted " +
		 " ORDER BY a.Id asc";
		
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next() ) {
			Archive arch = Archive.makeGlacierArchive(rs.getInt(1),
					rs.getString(2), rs.getString(3),
					rs.getInt(5), rs.getInt(6),
					rs.getLong(7), rs.getLong(8));
			ExpectedGlacierArchive glarch = new ExpectedGlacierArchive();
			glarch.archive = arch;
			glarch.salted  = rs.getString(4);
			archs.add(glarch);
		}
		rs.close();
		
		return archs;
		
	}	
	
	public Archive getArchive() { return archive; }
	public String getEncryptionKey() { return salted; }

}
