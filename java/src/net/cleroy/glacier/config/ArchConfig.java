package net.cleroy.glacier.config;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;

import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.PropertiesCredentials;


public class ArchConfig {
	
	
	private Properties extractProps(Properties master, String tag)
	{
		Properties _props = new Properties();
		String tagDot = tag + ".";
		for(String key: master.stringPropertyNames()) {
			if(key.startsWith(tagDot)) {
				String k = key.substring(tagDot.length());
				_props.setProperty(k, master.getProperty(key));
			}
		}
		return _props;
	}
	
	public class PartConfig implements PartitionConfig {
		private Properties props;
		protected PartConfig(Properties p, String tag) {
			props = extractProps(p, tag);
		}
		@Override
		public String getHostname() {
			
			return props.getProperty("Hostname");
		}
		@Override
		public String getRoot() {
			return props.getProperty("Root");
		}
		@Override
		public Integer getId() {
			// TODO Auto-generated method stub
			if(props.containsKey("Id")) {
				return new Integer(props.getProperty("Id"));
			}
			return null;
			
		}
		@Override
		public String[] getSubroots() {
			if(props.containsKey("SubRoots")) {
				return props.getProperty("SubRoots").split(",");
			}
			return new String[] {  "."  };
		}
		
		@Override
		public boolean getEncryptionFlag() {
			if(props.contains("Encryption")) {
				return props.getProperty("Encryption").equalsIgnoreCase("true");
			}
			return false;
		}
		@Override
		public String getGlacierEndPoint() {
			
			return props.getProperty("endPoint");
		}
		@Override
		public String getVaultArn() {
			
			return props.getProperty("vaultARN");
		}
		@Override
		public String[] getExtensions() {
			if(props.containsKey("Extensions")) {
				return props.getProperty("Extensions").toLowerCase().split(",");
			}
			return new String[] { "*" };
		}
		@Override
		public int getPreferredChunkSizeMB() {
			if(props.containsKey("preferredSizeMB")) {
				return Integer.parseInt(props.getProperty("preferredSizeMB"));
			}
			return 40;
		}
		
	}
	
	private byte[] encryptionKeyPer(String seed) throws NoSuchAlgorithmException {
	    MessageDigest md5  = MessageDigest.getInstance("MD5");
		if(seed.length()<20) {
			throw new RuntimeException("seed key is too short");
		}
		
		byte[] bseed = seed.getBytes();
		int n = bseed.length;
		int k16 = 1024*16;
		
		byte[] bkey1 = new byte[k16];
		byte[] bkey2 = new byte[k16];
		int a,b,j,k;
		for(int i = 0; i< k16; i++) {
			j = i%n;
			k = (i-j)/n;
			a = (((j+k)%n)+n)%n;
			b = (j-k)%n; 
			b = (((b-a)%n)+n)%n;
			bkey1[i] = bseed[a];
			bkey2[i] = bseed[b];			
		}
		
		byte[] h1, h2;
		md5.reset();md5.update(bkey1);
		h1 = md5.digest();
		md5.reset();md5.update(bkey2);
		h2 = md5.digest();
		
		byte[] hh = new byte[ h1.length + h2.length];
		//hh = new byte[16];
		for(int i=0;i<hh.length;i++) 
			hh[i] = (i<h1.length? h1[i] : h2[i-h1.length]);
		
		return hh;
	    
	}
	
	public Connection makeConnection() throws Exception
	{
		Class.forName("org.sqlite.JDBC");
	    Connection c = DriverManager.getConnection(this.jdbc);
	    c.setAutoCommit(false);
	    return c;
	      
	}
	

	String jdbc = null;
	String[] partitions = null;
	PartConfig[] configs = null;
	byte[] encryptionKey = null;
	PropertiesCredentials awsCredentials = null;
	
	public ArchConfig() throws Exception {
		
			Properties prop = new Properties();
			
		    InputStream in = ArchConfig.class.getClassLoader().
		    		getResourceAsStream("ArchConfig.properties");
		    prop.load(in);
		    in.close();
		    
		    this.jdbc = prop.getProperty("jdbc");
		    this.partitions = prop.getProperty("partitions").split(",");
		    this.configs = new PartConfig[ this.partitions.length ];
		    
		    for(int i = 0 ; i<this.partitions.length; i++) {
		    	this.configs[i] = new PartConfig(prop, this.partitions[i]);
		    }
		    
		    String key = prop.getProperty("encryptionKey");
		    if(key.startsWith("file://")) {
		    	String file = key.substring("file://".length());
		    	File ffile = new File(file);
		    	if(! ( ffile.exists() && ffile.isFile() && ffile.canRead())) {
		    		throw new RuntimeException("unable to file file " + file + " (or unable to read it)");
		    	}
		    	FileReader rd = new FileReader(ffile);
		    	char[] buffer = new char[2048];
		    	StringBuilder sb = new StringBuilder();
		    	int count = 0;
		    	while( (count = rd.read(buffer)) > 0 ) {
		    		sb.append(buffer, 0, count);
		    	}
		    	rd.close();
		    	key = sb.toString();
		    }
		    if(key == null) {
		    	key = "le petit chaperon rouge s'est fait vole sa glace par un juggernaut insolite. Mais, franchement, je m'en fous";
		    }
		    
		    this.encryptionKey = encryptionKeyPer(key);		
		    try {
			    awsCredentials = new PropertiesCredentials(
		                this.getClass().getClassLoader().getSystemClassLoader()
		                        .getResourceAsStream("ArchiveCredentials.properties"));
		    }catch(Exception issue) {
		    	issue.printStackTrace(System.err);
		    	throw new RuntimeException("Unable to get AWS credentials (ArchiveCredentials.properties ought to be in your classpath)");
		    }

	}
	
	public List<PartitionConfig> getPartitionConfigs() {
		List<PartitionConfig> list = new ArrayList<PartitionConfig>();
		for(PartitionConfig p: this.configs) list.add(p);
		return list;
	}
	
	public PropertiesCredentials getAwsCredentials() {
		return awsCredentials;
	}
	
	public byte[] getEncryptionKey() {
		byte[] k = new byte[ this.encryptionKey.length ];
		for(int i = 0; i<k.length;i++) k[i] = this.encryptionKey[i];
		return k;
	}
	

}
