package net.cleroy.glacier.archiving;

/** an archive path is a path (directory) in the archive */
public class ArchivePath {

	protected String root;
	protected String path;
	
	/** by convention it is -1 until we have its  ID in the database */
	protected int pathId = -1;
	
	public ArchivePath(String root, String subRootPath) {
		this.root = root;
		this.path = subRootPath;
	}
	
}
