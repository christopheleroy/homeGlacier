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
	private List<ArchiveMember> members;
	
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
		return sizeBytes;
	}
	
	public Iterator<ArchiveMember> getMembers() { return members.iterator(); }
	public int getSize() { return sizeBytes; }
	
	
}
