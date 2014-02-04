package net.cleroy.glacier;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.cleroy.glacier.config.ArchConfig;
import net.cleroy.glacier.config.PartitionConfig;

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
				(new cmd()).listArchiveOf( args[0].substring("listArchiveOf:".length()) );
				
				
			}
			System.out.println("Nothing found.");
		}

		

	}

}
