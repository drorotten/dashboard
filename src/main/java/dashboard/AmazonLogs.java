/*
 * Copyright (c) 2013 Dror. All rights reserved
 * <p/>
 * The software source code is proprietary and confidential information of Dror.
 * You may use the software source code solely under the terms and limitations of
 * the license agreement granted to you by Dror.
 */

package dashboard;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.zip.GZIPInputStream;

// Amazon
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;


public class AmazonLogs
{

   // ======================================  
   // Read Amazon CloudFront Logs (Zip files) 
   // 
   // ======================================
   public int readAmazonLogs(int n, 
                             String AWS_USER, String AWS_PASS, String bucketName, String DELETE_PROCESSED_LOGS,
                             String API_KEY, String TOKEN,
                             String apiuser, String apipass) throws Exception {
                             
        if (n < 1) return 0;  
        int eventsNumber = 0;
        String line = null;
        int begin = 0;
        int zips = 0;
        int deletedZips = 0;
        int mixpanelStatus = 0;
        String registrant = "";
        
        Mixpanel mix = new Mixpanel();
        //Whois w = new Whois();
        
        // Log files Bucket
        AWSCredentials credentials = new BasicAWSCredentials(AWS_USER,AWS_PASS);
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        
        // Set MARKER - from which Log File to start reading
        // listObjectsRequest.setMarker("E2DXXJR0N8BXOK.2013-03-18-10.ICK6IvaY.gz");
        
        BufferedReader br = null;
        
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        ObjectListing nextObjectListing = objectListing;
        zips = 0;
        Boolean more = true;
        if (objectListing == null) more = false;
                
        while (more) {
        // Reads 1000 files
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          // Handle  ZIP files        
          
          try { // Open and send to mixpanel events of one ZIP file  
            String key = objectSummary.getKey();
         
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
            // Extract ZIP and read Object to reader
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(object.getObjectContent())));
            zips++;      
            
            // Read the lines from the unzipped file, break it and send to Mixpanel
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) continue;
                if (line.trim().equals("")) continue;
                String[] values = line.split("\\s");  
                
                String eventTime = values[0] + " " + values[1];
                String ip = values[4];
                
                // WHOIS - Check registrant of this IP address
                //registrant = w.whois( ip, apiuser, apipass ); 
                
                String method = values[5];
                String fileName = values[7];
                String statusCode = values[8];
                String userAgent = values[10];
                String fName = fileName;
                
                if (fileName.contains("gigaspaces-")) {
                   begin = fileName.lastIndexOf("gigaspaces-") + 11;
                   fName = fileName.substring(begin, fileName.length()); 
                }

                eventsNumber++; 
                System.out.println(eventsNumber + ": " + eventTime + " " + ip );

                // ====================================================
                // Track the event in Mixpanel (using the POST import)
                // ====================================================
                mixpanelStatus = mix.postCDNEventToMixpanel(API_KEY, TOKEN, ip, "Cloudfront CDN", eventTime, method,  fileName, fName, userAgent, statusCode, registrant);
      
            } // while on ZIP file lines
     
            if (mixpanelStatus == 1 & DELETE_PROCESSED_LOGS.equals("YES")) { 
                  // Delete the CDN log ZIP file
                  s3Client.deleteObject(bucketName, key);
                  System.out.println("============ Deleted Zip " + zips + " ============"); 
                  deletedZips++;
            }
         } catch (IOException e) {
			
               e.printStackTrace();
               return eventsNumber;
		     } finally {
				     if (br != null) {
                br.close();
             }
   
             if (eventsNumber >= n) { 
                System.out.println("\n>>> " + eventsNumber + " events in " + zips + " Zip files. Deleted " + deletedZips + " Zip files.\n");
                return eventsNumber;
            }
         }
                
        } // for (continue to next ZIP file
        
        // If there are more ZIP files, read next batch of 1000
        if (objectListing.isTruncated()) {
            nextObjectListing = s3Client.listNextBatchOfObjects(objectListing);
            objectListing = nextObjectListing;
        } else 
            more = false; // no more files
        
       } // while next objectListing
        
       System.out.println("\n>>> " + eventsNumber + " events in " + zips + " Zip files. Deleted " + deletedZips + " Zip files.\n");
       return eventsNumber;
    }
}