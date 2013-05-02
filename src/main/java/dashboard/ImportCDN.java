/*
 * Copyright (c) 2013 Dror. All rights reserved
 * <p/>
 * The software source code is proprietary and confidential information of Dror.
 * You may use the software source code solely under the terms and limitations of
 * the license agreement granted to you by Dror.
 */

package dashboard;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Reads CDNs from Amazon CloudFront backet 
 * Import events to Mixpanel
 *
 */

public class ImportCDN
{
   static String TOKEN = "";
   static String API_KEY = "";
   static String bucketName = "";
   static String AWS_USER = "";
   static String AWS_PASS = "";
   static String DELETE_PROCESSED_LOGS = "";
   static String apiuser = "";
   static String apipass = "";
    
    public static void main(String[] args)
 
    {
      int n = 0;
      String propertiesFileName = "";
      AmazonLogs al = new AmazonLogs();
      
      // First argument - number of events to import
      if (args.length > 0) {
        try {
           n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
           System.err.println("First argument must be an integer");
           System.exit(1);
        }
      } else { 
           System.err.println("Please specify number of events to import.");
           System.exit(1);
      }

      // Second argument - properties file name
      if (args.length > 1) 
        propertiesFileName = args[1];
      else 
        propertiesFileName = "gigaDashboard.properties";
      
      // Read Properties file
      Properties prop = new Properties(); 
      try {
           //load a properties file
    		   prop.load(new FileInputStream(propertiesFileName));
         
           // Another option - load default properties from the Jar
           //prop.load(ImportCDN.class.getResourceAsStream("/gigaDashboard.properties"));
 
           //get the property values
           TOKEN = prop.getProperty("MIXPANEL_GIGA_PROJECT_TOKEN");
           API_KEY = prop.getProperty("MIXPANEL_GIGA_API_KEY");
           bucketName = prop.getProperty("S3_BUCKET_NAME");
           AWS_USER = prop.getProperty("AWS_USER");
           AWS_PASS = prop.getProperty("AWS_PASS");
           DELETE_PROCESSED_LOGS = prop.getProperty("DELETE_PROCESSED_LOGS");
           apiuser = prop.getProperty("DOMAINTOOLS_USER");
           apipass = prop.getProperty("DOMAINTOOLS_API_KEY");
                    
           //System.out.println("MIXPANEL PROJECT TOKEN = " + TOKEN);
       		//System.out.println("MIXPANEL API KEY = " + API_KEY);
       		System.out.println("DELETE_PROCESSED_LOGS = " + DELETE_PROCESSED_LOGS);
    		   System.out.println("S3_BUCKET_NAME = " + prop.getProperty("S3_BUCKET_NAME"));
       		//System.out.println("AWS_USER = " + prop.getProperty("AWS_USER"));
       		//System.out.println("AWS_PASS = " + prop.getProperty("AWS_PASS"));
           System.out.println( "===================");
    	} catch (IOException ex) {
    		ex.printStackTrace();
        System.err.println("Can't find Propertie file - " + propertiesFileName);
        System.err.println("Second argument must be properties file name");
        System.exit(1);
      }

      // READ AMAZON LOG FILES
      // AND IMPORT EVENTS TO MIXPANEL
      try {
         System.out.println("\n>>> Starting to import " + n + " events... \n");
         al.readAmazonLogs(n, AWS_USER, AWS_PASS, bucketName, DELETE_PROCESSED_LOGS, API_KEY, TOKEN, apiuser, apipass);
      } catch (Exception e) {
         e.printStackTrace();
      }   
    }    
}

