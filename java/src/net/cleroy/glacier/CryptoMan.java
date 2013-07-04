package net.cleroy.glacier;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.SecretKeySpec;

import net.cleroy.glacier.archiving.Archive;

public class CryptoMan {
	
	private static final char[] hexArray =
	   {'0','1','2','3','4','5','6','7','8','9',
		                'A','B','C','D','E','F'};
	
	
	/** make a Hexadecimal string from a byte array. e.g [ 0xAF, 0xDD, 0x12] => "AFDD12" 
	 */
	public static String bytesToHex(byte[] bytes) {
	    
	    char[] hexChars = new char[bytes.length * 2];
	    int v;
	    for ( int j = 0; j < bytes.length; j++ ) {
	        v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	/** makes a byte array from a Hexadecimal string, e.g "135ABC20" => [ 0x13, 0x5A, 0xBC, 0x20 ]
	 * 
	 * @param hexs: an hexadecimal string. Must contain only upper case A-F and numeric 0-9 and must have an even number characters, otherwise some of the bytes will be quite wrong... 
	 * @return the byte array, reading the string from left to right.
	 * @throws RuntimeException when @param hexs has a length that is not even
	 */
	public static byte[] hexToBytes(String hexs) {
		char[] hex = hexs.toCharArray();
		int length = hex.length / 2;
		if(hex.length %2 == 1) throw new RuntimeException("Hex string is not of an even size: " + hexs);
		byte[] raw = new byte[length];
		for (int i = 0; i < length; i++) {
		    int high = Character.digit(hex[i * 2], 16);
		    int low = Character.digit(hex[i * 2 + 1], 16);
		    int value = (high << 4) | low;
		    if (value > 127)
		    value -= 256;
		    raw[i] = (byte) value;
		}
		return raw;
	}


	private static CipherInputStream prepareCipher(byte[] encryptionKey, int cryptoMode, InputStream in) throws Exception {
		SecretKeySpec  key = new SecretKeySpec(encryptionKey, "AES");
		Cipher cif = Cipher.getInstance("AES/ECB/ISO10126Padding");
		cif.init(cryptoMode, key);
		CipherInputStream cip = new CipherInputStream(in, cif);
		return cip;
	}
	
    private static String xxcryptStreamsAndGetSHA256(byte[] encryptionKey, int cryptoMode, InputStream in, OutputStream out, boolean closeOutStream, boolean skipSHA256) throws Exception {
		
		CipherInputStream cip = prepareCipher(encryptionKey, cryptoMode, in);
		int k64 = 64*1024;
		byte[] buffer = new byte[k64];
		int count = 0;
		
		MessageDigest mdSHA256 = null;
		
		if(!skipSHA256) {
			mdSHA256 = MessageDigest.getInstance("SHA-256");
			mdSHA256.reset();
		}
		
		
		while( (count = cip.read(buffer))>0) {
			if(!skipSHA256) 
				mdSHA256.update(buffer, 0, count);
			out.write(buffer,0,count);
		}
		if(closeOutStream) 
			out.close();
		
		cip.close();
	
		return  skipSHA256 ? null : bytesToHex( mdSHA256.digest() );
	}
    
    public static String encryptStreamsAndGetSHA256(
    		byte[] encryptionKey, InputStream in, OutputStream out,
    		boolean closeOutStream) throws Exception {
    	return xxcryptStreamsAndGetSHA256(encryptionKey, Cipher.ENCRYPT_MODE, in, out, closeOutStream, false);
    }
    
    public static void decryptStreams(
    		byte[] encryptionKey,InputStream in, OutputStream out, 
    		boolean closeOutStream) throws Exception {
    	xxcryptStreamsAndGetSHA256(encryptionKey, Cipher.DECRYPT_MODE, in, out, closeOutStream, true);
    	
    }
	

}
