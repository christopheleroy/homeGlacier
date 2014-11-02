package net.cleroy.glacier.restoring;

import java.util.Date;

public class RetrievalGlacierArchive {
	
	String archiveId;
	String description;
	Date   creationDate;
	int size;
	String sha256TreeHash;
	
	public RetrievalGlacierArchive(String archiveId, String description, Date creationDate, int size, String sha256TreeHash) {
		this.archiveId = archiveId;
		this.description = description;
		this.creationDate = creationDate;
		this.size =size;
		this.sha256TreeHash = sha256TreeHash;
	}
	

}
