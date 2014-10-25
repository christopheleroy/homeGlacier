package net.cleroy.glacier.archiving;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;

import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;

public class Archiver {
	
	private PartitionConfig config;
	private HashMap<String, ArchivePath> paths;
	
	public Archiver(PartitionConfig config) {
		this.config = config;
		this.paths = new HashMap<String, ArchivePath>();
	}
	
	private ArchivePath findPath(String root, String subRootPath) {
		String p = subRootPath.replaceAll("//", "/").replaceAll("/$", "");
		
		
		if(this.paths.containsKey(p)) {
			return this.paths.get(p);
		}else{
			ArchivePath x = new ArchivePath(root, p);
			this.paths.put(p,x);
			return x;
		}
	}
	

	
	private void recurseCrawl(List<ArchiveMember> members, String root, String subRoot, String path)
	{
		File d = new File(root+"/"+subRoot+ (path == null ? "" : "/" + path)); 
		if(d.isDirectory()) {
			File[] listing =d.listFiles();
			String subPath = path == null  ? "": path;
			ArchivePath ap = findPath(root, subRoot + "/" + subPath); 
			
			for(File f : listing) {
				if(f.isDirectory() && 
						! f.getName().equals(".") && 
						! f.getName().equals("..")) {					
					recurseCrawl(members, root, subRoot, subPath+"/" + f.getName());
				}else{
					ArchiveMember m = new ArchiveMember();
					m.path = ap;
					m.filename = f.getName();
					m.fileSize = (int)f.length();
					m.timeStamp = f.lastModified();
					members.add(m);
				}
			}
		}
	}
	
	public List<ArchiveMember> crawl() {
		
		String root = config.getRoot();
		List<ArchiveMember> members = new ArrayList<ArchiveMember>();
		
		for(String subroot : config.getSubroots() ) {
			File s = new File(root + "/" + subroot);
			if(s.isDirectory()) {
				recurseCrawl(members, root, subroot, null);				
			}
		}		
		return members;
	}
	

	
	public List<ArchiveMember> loadFromDb(Connection c) throws SQLException {
		String sql = "SELECT p.path, p.id, m.filename, m.file_size, m.timest, m.sha256 " +
		 "FROM ARCHIVE a JOIN ARCHIVE_CONTENT m ON (m.ARCHIVE_ID = a.ID) " +
		 "JOIN PATHNAME p ON (m.PATH_ID = p.ID) " +
		 " WHERE  a.PARTITION_ID = " + config.getId();
		
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		
		String root = this.config.getRoot();
		List<ArchiveMember> mems = new ArrayList<ArchiveMember>();
		
		while(rs.next()) {
			String pp = rs.getString(1);
			int pp_id = rs.getInt(2);
			String fn = rs.getString(3);
			int fsize = rs.getInt(4);
			long tstamp = rs.getLong(5);
			String sha256 = rs.getString(6);
			
			ArchivePath p = findPath(root, pp);
			if(p.pathId <0) 
				p.pathId = pp_id;
			ArchiveMember m = new ArchiveMember();
			m.path = p;
			m.filename = fn;
			m.fileSize = fsize;
			m.timeStamp = tstamp;
			m.sha256 = sha256;
			
			mems.add(m);
		}
		
		return mems;
	}
	
	public List<Archive> makeNewArchive(List<ArchiveMember> currentlyArchived, List<ArchiveMember> currentFiles) {
		HashMap<String, ArchiveMember> currentFileIndex = new HashMap<String, ArchiveMember>();
		HashMap<String, ArchiveMember> filesToArchive = new HashMap<String, ArchiveMember>();
		for(ArchiveMember m : currentFiles) {
			String p = m.path.path + "/" + m.filename;
			currentFileIndex.put(p, m);
			filesToArchive.put(p, m);
		}
		
		for(ArchiveMember m: currentlyArchived) {
			String p = m.path.path + "/" + m.filename;
			if(filesToArchive.containsKey(p)) {
				ArchiveMember x = filesToArchive.get(p);
				if(x.fileSize == m.fileSize && x.timeStamp == m.timeStamp) {
					filesToArchive.remove(p);
				}
			}
		}
		long totalSize = 0L;
		for(ArchiveMember m : filesToArchive.values()) { totalSize += m.fileSize; }
		System.out.println("makeNewArchive: " + filesToArchive.size() + " files to be archived, totalling " + totalSize + " bytes.");
		
		String[] files = filesToArchive.keySet().toArray(new String[0]);
		Arrays.sort(files);
		
		List<Archive> archives = new ArrayList<Archive>();
		Archive a = new Archive();
		archives.add(a);
		long  sizeMax = this.config.getPreferredChunkSizeMB() * 1024L * 1024L;
		long aSize = 0L;
		long biggestArchive = 0L;
		
		for(String f : files) {
			ArchiveMember m = filesToArchive.get(f);
			if(m.fileSize > 2000*1000*1000) 
				continue; // for now skip files bigger than 550 Mb
			aSize = a.add(m);
			if(aSize>sizeMax) {
				if(aSize>biggestArchive) biggestArchive = aSize;
				a = new Archive();
				archives.add(a);
			}
		}
		if(aSize>biggestArchive) biggestArchive = aSize;
		
		System.out.println("makeNewArchive: will make " + archives.size() + " archives, biggest size = " + biggestArchive + " bytes.");
		
		return archives;
		
	}
	
	
	public void makeArchive(Archive arch, String outFile) throws IOException {
		
		int k64 = 64 * 1024;
		byte[] buffer = new byte[k64];
		FileInputStream fis = null;
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outFile));
		String root = this.config.getRoot();
		
		for(Iterator<ArchiveMember> it = arch.getMembers(); it.hasNext(); ) {
			ArchiveMember m = it.next();
			fis = null;
			try {
				fis  = new FileInputStream(root + "/" + m.path.path + "/" + m.filename);
			}catch(IOException x) {
				System.err.println("Will skip " + m.path.path + "/" + m.filename  +": exception " + x.toString());
				continue;
			}
			out.putNextEntry(new ZipEntry(m.path.path + "/" + m.filename));
			System.out.println("adding to archive: " + m.path.path + "/" + m.filename);
			int count = 0;
			while( (count = fis.read(buffer))>0) {
				out.write(buffer,0,count);
			}
			fis.close();
		}
		out.close();
	}
		
	public static String bytesToHex(byte[] bytes) {
	    final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	    char[] hexChars = new char[bytes.length * 2];
	    int v;
	    for ( int j = 0; j < bytes.length; j++ ) {
	        v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	public static byte[] hexToBytes(String hexs) {
		char[] hex = hexs.toCharArray();
		int length = hex.length / 2;
		if(hex.length %2 == 1) throw new RuntimeException("Hex string is not of an even size: " + hexs);
		byte[] raw = new byte[length];
		for (int i = 0; i < length; i++) {
		    int high = Character.digit(hex[i * 2], 16);
		    int low = Character.digit(hex[i * 2 + 1], 16);
		    int value = (high << 4) | low;
		    if (value > 127)
		    value -= 256;
		    raw[i] = (byte) value;
		}
		return raw;
	}

		
	public String encryptArchiveAndGetSHA256(byte[] encryptionKey, Archive arch, String archiveFile, String encryptedFile) throws Exception {
		
		SecretKeySpec key = new SecretKeySpec(encryptionKey, "AES");
		Cipher cif = Cipher.getInstance("AES/ECB/ISO10126Padding");
		cif.init(Cipher.ENCRYPT_MODE, key);
		CipherInputStream cip = new CipherInputStream(new FileInputStream(archiveFile), cif);
		int k64 = 64*1024;
		byte[] buffer = new byte[k64];
		int count = 0;
		
		MessageDigest mdSHA256 = MessageDigest.getInstance("SHA-256");
		mdSHA256.reset();
		FileOutputStream fos = new FileOutputStream(encryptedFile);
		
		while( (count = cip.read(buffer))>0) {
			mdSHA256.update(buffer, 0, count);
			fos.write(buffer,0,count);
		}
		fos.close();
		cip.close();
	
		arch.sha256 = bytesToHex( mdSHA256.digest() );
		return arch.sha256;
	}
			
			
		
	public void storePaths(Connection c) throws SQLException
	{
		List<ArchivePath> newPaths = new ArrayList<ArchivePath>();
		for(ArchivePath p : this.paths.values()) {
			if(p.pathId<0) {
				newPaths.add(p);
			}
		}
		
		if(newPaths.size()>0) {
			String sql = "INSERT into PATHNAME(PARTITION_ID, PATH) VALUES("  + this.config.getId().toString() + ",?)";
			String sqlVerify = "SELECT ID FROM PATHNAME where PARTITION_ID = " + this.config.getId().toString() + " AND PATH = ?";
			PreparedStatement stInsert = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement stVerify = c.prepareStatement(sqlVerify);
			
			for(ArchivePath p: newPaths) {
				ResultSet rs = null;
				stVerify.clearParameters();
				stVerify.setString(1, p.path);
				rs = stVerify.executeQuery();
				if(rs.next()) {
					p.pathId = rs.getInt(1);
				}else{
					rs.close();
					stInsert.clearParameters();
					stInsert.setString(1, p.path);
					stInsert.execute();
					
					rs = stInsert.getGeneratedKeys();
					if(rs.next()) {
						p.pathId = rs.getInt(1);
					}else{
						System.out.println("unable to Get Generated Id for PATHNAME " + p.path);
					}
				}
			}
			c.commit();
		}
	}
	
	public void storeArchiveContent(Connection c, Archive a) throws SQLException
	{
		String sqlInsertArchive = "INSERT into ARCHIVE(PARTITION_ID) VALUES(" + this.config.getId().toString() + ")";
		PreparedStatement stInsert = c.prepareStatement(sqlInsertArchive, Statement.RETURN_GENERATED_KEYS);
		
		int archiveId = -1;
		
		stInsert.clearParameters();
		stInsert.execute();
		ResultSet rs = stInsert.getGeneratedKeys();
		if(rs.next()) {
			archiveId = rs.getInt(1);
			a.archiveId = archiveId;
		}else{
			System.out.println("unable to Get Generated Id for ARCHIVE ");
			
		}
		
		String sqlInsertContent = "INSERT into ARCHIVE_CONTENT(ARCHIVE_ID, PATH_ID, FILENAME, TIMEST, FILE_SIZE, SHA256)" +
				" VALUES (" + archiveId + ", ?, ?, ?, ?, ?)";
		
		PreparedStatement stInsertArch = c.prepareStatement(sqlInsertContent);
		
		for(Iterator<ArchiveMember> it = a.getMembers(); it.hasNext();) {
			ArchiveMember m = it.next();
			stInsertArch.clearParameters();
			stInsertArch.setInt(1, m.path.pathId);
			stInsertArch.setString(2, m.filename);
			stInsertArch.setLong(3, (long)m.timeStamp);
			stInsertArch.setInt(4, m.fileSize);
			if(m.sha256 == null) { 
				stInsertArch.setNull(5, Types.CHAR);
			}else{
				stInsertArch.setString(5, m.sha256);
			}
			stInsertArch.execute();
		}
		c.commit();
	}
	
	public void storeFinalArchiveInfo(Connection c, Archive a, byte[] encryptionKey) throws SQLException
	{
		String sqlUpdateArch = "UPDATE ARCHIVE SET SHA256=?, VAULT_NAME=?, GLACIER_ID=?, SALTED=? WHERE  ID = ?";
		PreparedStatement stUpdate = c.prepareStatement(sqlUpdateArch);
		
		stUpdate.clearParameters();
		stUpdate.setString(1, a.sha256);
		stUpdate.setString(2, this.config.getVaultArn());
		stUpdate.setString(3,a.glacierId);
		stUpdate.setString(4,bytesToHex(encryptionKey));
		stUpdate.setInt(5, a.archiveId);
		
		stUpdate.execute();
		c.commit();
		
	}

	private static void timeReport(int sizeBytes, long t0, long tA, long tE, long tU, long tS) {
		System.out.println("** ************************************************************** **");
		System.out.println("**  Archive of " + (((double)sizeBytes)/1000.0) + " kilobytes -- took: ");
		System.out.println("** " + (tA-t0) + " ms to build");
		System.out.println("** " + (tE-tA) + " ms to encrypt  (" + (tE-t0) + " ms to build+encrypt");
		if(tU > tE) {
			System.out.println("** " + (tU-tE) + " ms to upload");
			System.out.println("** " + (tU-tS) + " ms to save status");
			System.out.println("** " + (tS-t0) +" ms overall, that is " + ( (tS-t0)/1000 ) + " seconds overall");
		}
		System.out.println("** ************************************************************** **");
		
	}
	
	public void cycle(ArchConfig archConf, int archiveMax, int megaBytesMax) throws Exception {
		long byteMax = (long)megaBytesMax * 1024L * 1024L;
		long byteTotal = 0L;
		int archiveNumber = 0;
		
		Connection cc = archConf.makeConnection();
		List<ArchiveMember> filesInArchive = this.loadFromDb(cc);
		List<ArchiveMember> currentFiles = crawl();
		List<Archive> archives = makeNewArchive(filesInArchive, currentFiles);
		
		storePaths(cc);
		byte[] encKey = archConf.getEncryptionKey();
		
		for(Archive arch: archives) {
			if(archiveMax> ++archiveNumber && byteMax>byteTotal) {
				storeArchiveContent(cc, arch);
				String fzip = "c:/temp/glacier-" + arch.archiveId + ".zip";
				String fbze = "c:/temp/glacier-" + arch.archiveId + ".bze";
				long t0 = System.currentTimeMillis();
				makeArchive(arch, fzip); System.out.println("Archive made: " + fzip);
				long tA = System.currentTimeMillis();
				encryptArchiveAndGetSHA256(encKey, arch, fzip, fbze);System.out.println("Archive encrypted: " + fbze);
				long tE = System.currentTimeMillis();
				if(ArchiveUploader.upload(archConf, this.config, arch, fbze)) {
					long tU = System.currentTimeMillis();
					System.out.println("Encrypted archive uploaded");
					storeFinalArchiveInfo(cc, arch, encKey);
					long tS = System.currentTimeMillis();
					timeReport(arch.getSize(), t0, tA, tE, tU, tS);
				}else{
					timeReport(arch.getSize(), t0, tA, tE, tE, tE);
				}
				byteTotal += arch.getSize();
			}
		}
		System.out.println("Stopping the archive process after " + archiveMax + " archives and/or " + byteTotal + " bytes archived.");
	}
	
	
	public static void main(String args[]) throws Exception {
		if(args.length==0) {
			System.err.println("Usage: archiver partitionID");
		}else if("clean-up".equals(args[0])) {
			ArchConfig cfg = new ArchConfig();
			Connection c = cfg.makeConnection();
			Statement st = c.createStatement();
			st.executeUpdate("DELETE FROM ARCHIVE_CONTENT WHERE ARCHIVE_ID IN (SELECT ID FROM ARCHIVE WHERE GLACIER_ID is NULL)");
			c.commit();
		}else{
			// crash early if not all args are numeric
			for(int i = 0; i< args.length; i++) {
				int partId = Integer.parseInt(args[i]);
			}
			for(int i = 0; i< args.length; i++) try {
				int partId = Integer.parseInt(args[i]);
			
				ArchConfig cfg = new ArchConfig();
				for(PartitionConfig pcfg: cfg.getPartitionConfigs()) {
					if(pcfg.getId() == partId) {
						String   hn = pcfg.getHostname();
						String root = pcfg.getRoot();
						File r = new File(root);
						if(r.exists() && r.isDirectory()) {
							Archiver archiver = new Archiver(pcfg);
							archiver.cycle(cfg, 200, 10000);
						}
					}
				}
			}catch(Exception x) {
				x.printStackTrace();
			}
		}
	
	}
	
}
