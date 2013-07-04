package net.cleroy.glacier.archiving;


/** an ArchiveMember is a file on a filesystem that is to be added to an archive and later added.
 * @author leroych2
 *
 */
public class ArchiveMember {
	
	protected ArchivePath path = null;
	protected String filename = null;
	protected int fileSize = -1;
	protected long timeStamp = -1L;
	protected String sha256 = null;
	
	
	
	
	
	
}
