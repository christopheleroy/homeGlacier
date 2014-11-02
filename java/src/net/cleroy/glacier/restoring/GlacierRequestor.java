package net.cleroy.glacier.restoring;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.util.StringInputStream;

import net.cleroy.glacier.CryptoMan;
import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;

public class GlacierRequestor {


	static public class ArchiveStatusJson {
		public String ArchiveId, ArchiveDescription, CreationDate, SHA256TreeHash;
		public int Size;
	}
	
	
	public static InitiateJobResult requestVaultListing(ArchConfig cfg, PartitionConfig pconf) {
		AmazonGlacierClient client = new AmazonGlacierClient(cfg.getAwsCredentials());
		
		client.setEndpoint(pconf.getGlacierEndPoint());
		
		JobParameters jp = new JobParameters("JSON", "inventory-retrieval", null, "List Vault " + pconf.getVaultArn());
		String vault = pconf.getVaultArn();

		InitiateJobRequest req = new InitiateJobRequest("981646003372", vault, jp);
		
		InitiateJobResult reqResu = client.initiateJob(req);
		System.err.println("InitiatedJob: " + reqResu.getJobId() + " for"  + reqResu.getLocation());
		return reqResu;
		
	}
	
	public static GetJobOutputResult requestJobOutput(ArchConfig cfg, PartitionConfig pconf, String jobId) {
		
		AmazonGlacierClient client = new AmazonGlacierClient(cfg.getAwsCredentials());
		
		client.setEndpoint(pconf.getGlacierEndPoint());
		DescribeJobRequest djr = new DescribeJobRequest("981646003372", pconf.getVaultArn(), jobId);
		
		DescribeJobResult jrd = client.describeJob(djr);
		
		if(jrd.isCompleted()) {
			GetJobOutputRequest gjoReq = new GetJobOutputRequest("981646003372", pconf.getVaultArn(), jobId, "");
			GetJobOutputResult rs = client.getJobOutput(gjoReq);
			return rs;
		}
		System.err.println("Job is not completed yet: " + jobId);
		
		return null;
	}
	
	public static List<RetrievalGlacierArchive> readArchiveStatusFromRetrievalJob(GetJobOutputResult res) 
	throws Exception {
		
		InputStream is = res.getBody();
		ObjectMapper om = new ObjectMapper();
		JsonNode jnX = om.readTree(is);
		Iterator<JsonNode> jnArchList  = jnX.get("ArchiveList").getElements();
		
		List<RetrievalGlacierArchive> list = new ArrayList<RetrievalGlacierArchive>();
		
		for(JsonNode jn = null; jnArchList.hasNext();) {
			jn = jnArchList.next();
			ArchiveStatusJson arch = om.readValue(jn,  ArchiveStatusJson.class);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date cDate = sdf.parse(arch.CreationDate);
			RetrievalGlacierArchive garch = new RetrievalGlacierArchive(arch.ArchiveId, arch.ArchiveDescription, 
					cDate, arch.Size, arch.SHA256TreeHash);
			list.add(garch);
		}
		is.close();
		return list;
	}
		
	public static void writeRetrievedArchive(GetJobOutputResult res, int archiveId, String filename, boolean overwrite)
	throws Exception {
		InputStream is = res.getBody();
		File f = new File(filename);
		if(f.exists()) {
			if(f.isDirectory()) {
				throw new RuntimeException("Proposed file is a directory: " + filename);
			}else{
				if(!overwrite) {
					throw new RuntimeException("file exists: " + filename);
				}
			}
		}
		
		ArchConfig cfg = new ArchConfig();
		Connection cc = cfg.makeConnection();
		String encryptionKey = null;
		
		String sql = "SELECT SALTED from ARCHIVE where ID = ?";
		PreparedStatement pStmt = cc.prepareStatement(sql);
		pStmt.setInt(1, archiveId);
		
		ResultSet rs = pStmt.executeQuery();
		if(rs.next()) {
			encryptionKey = rs.getString(1);
		}
		if("".equals(encryptionKey)) {
			encryptionKey = null;
		}
		
		byte[] keyBytes = encryptionKey == null ? null :
			CryptoMan.hexToBytes(encryptionKey);
		
		File fx = keyBytes == null ? f : new File(filename + ".bze");
		FileOutputStream fos = new FileOutputStream(fx);
		

		int kb128 = 1024*128;
	
		byte[] buffer = new byte[kb128];
		int read = 0;
		
		while( (read = is.read(buffer)) > 0) {
			fos.write(buffer, 0, read);
		}
		fos.close();
		is.close();
		
		if(keyBytes != null) {
			FileOutputStream fos2 = new FileOutputStream(fx);
			FileInputStream fis = new FileInputStream(f);
			CryptoMan.decryptStreams(keyBytes, fis, fos2, true);
			fx.delete();
		}
	}

	
	public static void requetJobResults(ArchConfig cfg, PartitionConfig pconf) throws Exception {
		String job1 = "-oFRL_XIFaqvm_cEGqE8hdK8CiF_YSNRosRK9au1aO3iznC9X-0bx2UJJS4OJjY2BZK89IbDKDLYbbvco5JAVjPYqdXD";
		String job2 = "PsWnhzsVCPHNH8gVLHjukkYWt3atkig6Yb_e5bx97bukCivAUL4SpYTqteNbYtJgKpeueSwiN2zuQtluOcyQ7Kiv6tGO";
		
		
		AmazonGlacierClient client = new AmazonGlacierClient(cfg.getAwsCredentials());
		
		client.setEndpoint(pconf.getGlacierEndPoint());
		
		DescribeJobRequest djr = new DescribeJobRequest("981646003372", pconf.getVaultArn(), job1);
		DescribeJobResult jrd = client.describeJob(djr);
		
		System.err.println(jrd.getAction());
		System.err.println(jrd.getCreationDate());
		System.err.println(jrd.getSNSTopic());

		System.err.println(jrd.getStatusCode());

		System.err.println(jrd.getStatusMessage());
		if(jrd.getStatusCode().equals("Succeeded")) {
			String range = "0-" + (1024*1024 -1);
			GetJobOutputRequest gjoReq = new GetJobOutputRequest("981646003372", pconf.getVaultArn(), job1, range);
			GetJobOutputResult rs = client.getJobOutput(gjoReq);
			System.err.println(rs.getChecksum());
			System.err.println(rs.getContentType());
			System.err.println(rs.getStatus());
			InputStream is = rs.getBody();
//			BufferedReader reader = new BufferedReader( new InputStreamReader(is));
//			StringBuilder  bob = new StringBuilder();
//			String chunk;
//			while( (chunk = reader.readLine())!=null) {
//				System.out.println(chunk);
//				bob.append(chunk);
//			}
//			reader.close();
//			is.close();
//			String json = bob.toString();
			//JsonNode jn = (new ObjectMapper()).readTree(is);
			ObjectMapper om = new ObjectMapper();
			JsonNode jn = om.readTree(is);
			
			Iterator<JsonNode> jnArchList  = jn.get("ArchiveList").getElements();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			ArchiveStatusJson barch0 = new ArchiveStatusJson();
			ArchiveStatusJson barch1 = new ArchiveStatusJson();
			barch0.Size = barch1.Size = -1;
			
			Date now = new Date();
			for(JsonNode jn0 = null; jnArchList.hasNext();) {
				jn0 = jnArchList.next();
				System.err.println(jn0.toString());
				ArchiveStatusJson arch = om.readValue(jn0, ArchiveStatusJson.class);
				
				Date archDate = sdf.parse(arch.CreationDate);
				long age = now.getTime() - archDate.getTime();
				age /= (86400*1000);
				
				int sizeKb = arch.Size / 1024;
				int sizeMb = sizeKb / 1024;
				String size = sizeMb>7 ? sizeMb + " MB" : sizeKb + " kB";
				
				System.out.println(arch.ArchiveId.substring(0,15)+ "..., size="+ size + ", " + age + " days old");
				if(barch1.Size < arch.Size || barch0.Size < arch.Size) {
					barch1 = arch;
					if(barch1.Size > barch0.Size) {
						arch = barch0;
						barch0 = barch1;
						barch1 = arch;
					}
				}
				
			}
			System.err.println("Biggest: " + barch0.ArchiveId);
			System.err.println("2nd biggest: " + barch1.ArchiveId);
			
			JobParameters jp0 = new JobParameters().withType("archive-retrieval"). withArchiveId(barch0.ArchiveId).withDescription("biggest");
			InitiateJobRequest ijrq0 = new InitiateJobRequest("981646003372", pconf.getVaultArn(),jp0); 
			
			InitiateJobResult ijrs0 =  client.initiateJob(ijrq0);
			
			System.err.println("InitiatedJob: " + ijrs0.getJobId() + " for"  + ijrs0.getLocation());

			
			JobParameters jp1 = new JobParameters().withType("archive-retrieval"). withArchiveId(barch1.ArchiveId).withDescription("2nd biggest");
			InitiateJobRequest ijrq1 = new InitiateJobRequest("981646003372", pconf.getVaultArn(),jp1); 
			
			InitiateJobResult ijrs1 =  client.initiateJob(ijrq1);
			System.err.println("InitiatedJob: " + ijrs1.getJobId() + " for"  + ijrs0.getLocation());

			
		}
		
	
	}

	
	public static void main(String[] argv) throws Exception { //requetJobResults(new ArchConfig(), new ArchConfig().getPartitionConfigs().get(0));}
	}
	
}
