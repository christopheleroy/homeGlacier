package net.cleroy.glacier.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;

public class Ex1 {
	
	String endPoint;
	String vaultName;
	AWSCredentials credentials;
	
	String ex1ArchiveId;
	String snsTopicARN;
	
	String jobId;
	String jobFileOutput;
	
	public  void init() throws IOException {
		Properties prop = new Properties();
	    InputStream in = Ex1.class.getResourceAsStream("EX1.properties");
	    prop.load(in);
	    in.close();
	    
	    endPoint  = prop.getProperty("endPoint");
	    vaultName = prop.getProperty("vaultName");
	    
	    
	    
	    ex1ArchiveId = prop.getProperty("ex1Archive");
	    snsTopicARN = prop.getProperty("snsTopicArn");
	    
	    jobId = prop.getProperty("awsArchiveListJobId");
	    jobFileOutput = prop.getProperty("jobFileOutput");
	    
	    credentials = new PropertiesCredentials(
                Ex1.class
                        .getResourceAsStream("AwsCredentials.properties"));

	    
	}
	
	public  void doUpload(String archiveName, String archiveToUpload) throws AmazonServiceException, AmazonClientException, FileNotFoundException 
	{
		
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint(endPoint);
		ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
		String archiveId = atm.upload(vaultName, archiveName, new File(archiveToUpload)).getArchiveId();
		System.out.println("Received archiveId " + archiveId);
	}
	
	public void doLookUp()
	{
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint(endPoint);
		
		InitiateJobRequest initJobRequest = new InitiateJobRequest()
	    .withVaultName(vaultName)
	    .withJobParameters(
	            new JobParameters()
	                .withType("inventory-retrieval")
	                .withSNSTopic(snsTopicARN)
	      );

		InitiateJobResult initJobResult = client.initiateJob(initJobRequest);
		String jobId = initJobResult.getJobId();
		System.out.println("Job Id = " + jobId );
		
	}
	
	public void doDestroy()
	{
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint(endPoint);
		ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
		
		client.deleteArchive(new DeleteArchiveRequest()
			.withVaultName(vaultName)
			.withArchiveId(ex1ArchiveId));
		
		System.out.println("Deleted archive successfully.");

	}
	
	public void retrieveJobResults() throws IOException
	{
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint(endPoint);
		GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
        	.withVaultName(vaultName)
        	.withJobId(jobId);
		
		GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

	    FileWriter fstream = new FileWriter(jobFileOutput);
	    BufferedWriter out = new BufferedWriter(fstream);
	    BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));            
	    String inputLine;
	    try {
	        while ((inputLine = in.readLine()) != null) {
	            out.write(inputLine);
	        }
	    }catch(IOException e) {
	        throw new AmazonClientException("Unable to save archive", e);
	    }finally{
	        try {in.close();}  catch (Exception e) {}
	        try {out.close();}  catch (Exception e) {}             
	    }
	    System.out.println("Retrieved inventory to " + jobFileOutput);
	}
	
	
	public static void main(String args[]) throws Exception {
		Ex1 ex1 = new Ex1();
		ex1.init();
		//ex1.doUpload("Example1", "C:/Users/leroych2/Videos/Looxcie/05142013_1145/clip0004.mp4");
		//ex1.doLookUp();
		ex1.retrieveJobResults();
		
	}
	
	
}
