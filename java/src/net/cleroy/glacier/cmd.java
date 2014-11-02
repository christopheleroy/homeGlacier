package net.cleroy.glacier;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobResult;

import net.cleroy.glacier.archiving.Archive;
import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;
import net.cleroy.glacier.restoring.ExpectedGlacierArchive;
import net.cleroy.glacier.restoring.GlacierRequestor;

public class cmd {
	
	public enum ConfigItem {
		vaultARN, hostname, root, id
	}
	
	public Map<String, String> listInConfigs(ConfigItem what) throws Exception
	{
		ArchConfig cfg = new ArchConfig();
		
		HashMap<String, String> listWhat = new HashMap<String, String>();
		List<PartitionConfig> parts = cfg.getPartitionConfigs();
		for(PartitionConfig part: parts) {
			String  id = part.getId().toString();
			String val = id;
			switch(what) {
			case vaultARN: val = part.getVaultArn(); break;
			case hostname: val = part.getHostname(); break;
			case root:     val = part.getRoot(); break;
			};
			listWhat.put(id, val);
		}
		
		return listWhat;
	}
	
	public void listArchiveOf(String s_id) throws Exception {
		int id =  Integer.parseInt(s_id);
		ArchConfig cfg = new ArchConfig();
		
		Connection conn = cfg.makeConnection();
		List<ExpectedGlacierArchive> allArchives = ExpectedGlacierArchive.loadAllArchivesFromDb(conn, id);
		
		for(ExpectedGlacierArchive arch: allArchives) {
			Archive a = arch.getArchive();
			System.out.println(a.toString());
		}		
	}
	
	public void requestVaultList(String s_id) throws Exception {
		int id =  Integer.parseInt(s_id);
		

		ArchConfig cfg = new ArchConfig();
		List<PartitionConfig> pconfs = cfg.getPartitionConfigs();
		for(PartitionConfig pconf: pconfs) {
			if(pconf.getId().intValue() == id) {
				System.err.println("Requesting for " + pconf.getVaultArn());
				InitiateJobResult res = GlacierRequestor.requestVaultListing(cfg, pconf);
				System.err.println("JobId: " + res.getJobId());
			}
		}
	}
	
	public void writeRetrievedArchive(String conf_id, String archiveId, String jobId, String filename) throws Exception {
		ArchConfig cfg = new ArchConfig();
		int cId = Integer.parseInt(conf_id);
		int archiveId_i = Integer.parseInt(archiveId);
		for(PartitionConfig pconf: cfg.getPartitionConfigs()) {
			if(pconf.getId().intValue() == cId) {
				GetJobOutputResult res = GlacierRequestor.requestJobOutput(cfg, pconf, jobId);
				
				if(res != null) {
					System.err.println(res.getStatus());
					GlacierRequestor.writeRetrievedArchive(res, archiveId_i, filename, true);
				}else{
					System.err.println("the job is not ready");
				}
				return;
			}
		}
		System.err.println("The partition " + conf_id + " was not found");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length>0) {
			cmd cc = new cmd();
			if(args[0].startsWith("list:")) {
				String what = args[0].substring("list:".length());
				Map<String,String> list = null;
				for(ConfigItem it: ConfigItem.values()) {
					if(it.toString().equals(what)) {
						list = cc.listInConfigs(it);
					}
				}
				if(list!=null) {
					for(String id: list.keySet()) {
						System.out.println(id + ":  " + list.get(id));
					}
					return;
				}
			}
			if(args[0].startsWith("encryptionKey")) {
				ArchConfig cfg = new ArchConfig();
				byte[] key = cfg.getEncryptionKey();
				if(args[0].toLowerCase().endsWith("hex")) {
					System.out.println(CryptoMan.bytesToHex(key));
				}else if(args[0].toLowerCase().endsWith("length")) {
					System.out.println(key.length + " bytes = " + (8*key.length)+ " bits");
				}else{
					System.out.println(key);
				}
				
				return;
			}
			
			if(args[0].startsWith("listArchiveOf:")) {
				String n[] = args[0].substring("listArchiveOf:".length()).split(",");
				cmd cmdx = new cmd();
				
				for(String s : n ) {
					System.out.println("===================== config " + s + " =============================");
					cmdx.listArchiveOf( s );
				}
				System.out.println("----------------------------------------------------------------------");
				return;
				
			}
			
			if(args[0].startsWith("listVaultContent:")) {
				String n[] = args[0].substring("listVaultContent:".length()).split(",");
				cmd cmdx = new cmd();
				for(String i: n) {
					cmdx.requestVaultList(i);
				}
				return;
			}
			
			if(args[0].startsWith("retrieveArchive:")) {
				String archiveId = args[0].substring("retrieveArchive:".length());
				if(args.length < 4) {
					System.err.println("for 'retrieveArchive:n (n=archiveID) provide the jobID, the partition number and a filename");
				}else{
					cmd cmdx = new cmd();
					
					cmdx.writeRetrievedArchive(args[1], archiveId, args[2], args[3]);
				}
				return;
			}
			
			System.out.println("Nothing found.");
		}

		

	}

}
