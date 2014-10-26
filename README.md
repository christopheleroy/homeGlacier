homeGlacier
===========
Home Glavier is my week-end project to leverage Glacier, Amazon Web Services (AWS) solution for archiving files, for my home back-up.
Since this is in the cloud everything you archive is encrypted with AES (I use 32 bytes (256 bits) keys at the moment).

Home Glacier is designed with a family's data archive needs. Not terabytes, not huge files, but perhaps hundreds of files a week.
AWS Glacier is great for archiving, but not very fast for retrieving. But AWS is relativily cheap on the gigabyte.
Perhaps around $1 per month per 100 gigabytes stored. It will charge you a little more to retrieve the data though, 
and the data will be coming to you asynchronously. (so don't use AWS Glacier in place of AWS S3 for example).


Home Glacier allows you to configure a number of "Partitions" - a number of directories to scan.
It maintains the inventory of files already archived in a sqllite database - so every day, it will find the new files
or the files that have changed and archive them.
The archival is done by storing "archives" in Glacier in a designated Glacier Vault. An archive is a zip file containing a number
of the file in your partitions. The archive (zip file) is encrypted with AES (256 bits).
The encrypted archive must be supplied to Glacier with a SHA256 signaure (checksum). 
The sqllite database (a single binary file on your computer) contains the encryption keys and the sha256 signatures.
Never lose the sqlite database!

This is written in Java, using the Amazon provided AWS jars, a jar for sqllite. 
I think that by default it will require an AES key of 32 bytes which is not supported by the regular Java distribution,
so  you have install the Java Cryptograhy Extension (JCE) (which takes 2 minutes), or perhaps hack the code to go to 16 bytes (128 bits).
See http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html


