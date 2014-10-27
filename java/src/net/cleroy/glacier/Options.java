package net.cleroy.glacier;

public class Options {
	
	private boolean keepZips = false;
	private boolean keepBZEs = false;
	
	public static Options byCommandLine() {
		Options opts = new Options();
		
		String keeps = System.getProperty("keep", "none").toLowerCase();
		
		if(keeps.indexOf("zip")>=0) {
			opts.keepZips = true;
		}
		if(keeps.indexOf("bze")>=0) {
			opts.keepBZEs = true;
		}
		
		
		return opts;
				
	}
	/** whether we should keep the zip files that are prepared for uploads into Glacier */
	public boolean keepZips() { return keepZips; }
	/** whether we should keep the BZE files (encrypted zip files) that are prepared for uploads into Glacier */
	public boolean keepBZEs() { return keepBZEs; }

}
