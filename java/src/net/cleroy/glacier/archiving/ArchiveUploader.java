package net.cleroy.glacier.archiving;

import java.io.File;
import java.io.FileNotFoundException;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;

import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;

public class ArchiveUploader {
	
	public static boolean  upload(ArchConfig config, PartitionConfig pconf, Archive a, String encryptedFile) throws FileNotFoundException {
		
		AmazonGlacierClient client = new AmazonGlacierClient(config.getAwsCredentials());
		client.setEndpoint( pconf.getGlacierEndPoint() );
		ArchiveTransferManager atm = new ArchiveTransferManager(client, config.getAwsCredentials());
		
		String archiveId = atm.upload(pconf.getVaultArn(), "archive", new File(encryptedFile)).getArchiveId();
		
		a.glacierId = archiveId;
		return true;
		
	}
	
	

}
