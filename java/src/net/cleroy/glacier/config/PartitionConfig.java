package net.cleroy.glacier.config;

public interface PartitionConfig {
	public String getHostname();
	
	public String getRoot();
	
	public Integer getId();
	
	public String[] getSubroots();
	
	public int getPreferredChunkSizeMB();
	
	public boolean getEncryptionFlag();
	
	public String getGlacierEndPoint();
	public String getVaultArn();
	
	public String[] getExtensions();
	

}
