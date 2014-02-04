package net.cleroy.glacier.archiving;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Archive {
	
	protected int archiveId;
	protected boolean inGlacier;
	protected String glacierId;
	protected String sha256;
	private int sizeBytes;
	private long earliestTimestamp = -1L;
	private long latestTimestamp = -1L;
	private List<ArchiveMember> members;
	private int memberNumber = 0;
	
	
	public static Archive makeGlacierArchive(int archiveId, String glacierId, String sha256, 
			int numberOfMembers, int totalBytes,
			long earliestTimestamp, long latestTimestamp) {
		return new Archive(archiveId, glacierId, sha256, numberOfMembers, totalBytes,
				earliestTimestamp, latestTimestamp);
	}
	
	protected Archive(int archiveId, String glacierId, String sha256, 
			int numberOfMembers, int totalBytes,
			long earliestTimestamp, long latestTimestamp) {
		this.archiveId = archiveId;
		this.glacierId = glacierId;
		this.sha256 = sha256;
		this.earliestTimestamp = earliestTimestamp;
		this.latestTimestamp   = latestTimestamp;
		this.memberNumber = numberOfMembers;
		this.sizeBytes = totalBytes;
		this.inGlacier = true;
		this.members = null;
	}
	
	public Archive() { 
		archiveId = -1;
		inGlacier = false;
		glacierId = null;
		sizeBytes = 0;
		members = new ArrayList<ArchiveMember>();
	}
	
	public int add(ArchiveMember m) {
		members.add(m);
		sizeBytes += m.fileSize;
		if(earliestTimestamp > m.timeStamp)
			earliestTimestamp = m.timeStamp;
		
		if(latestTimestamp == -1L || latestTimestamp < m.timeStamp)
			latestTimestamp = m.timeStamp;
		
		memberNumber++;
		return sizeBytes;
	}
	
	public Iterator<ArchiveMember> getMembers() { return members.iterator(); }
	public int getSize() { return sizeBytes; }
	public String getSHA256() { return sha256; }
	public long getEarliestTimestamp() { return earliestTimestamp; }
	public long getLatestTimestamp() { return latestTimestamp; }
	public int getNumberOfFiles() { return memberNumber; }
	/** returns the number of bytes the archive will contains (represent).
	 * The archive file may be smaller or larger, as it is a zip file containing many files.
	 * The size is the sum of the size of the files. Zip may reduce that, or increase by a little.
	 * 
	 * @return size in bytes
	 */
	public int archiveSizeBytes() { return sizeBytes; }
	
	
}
