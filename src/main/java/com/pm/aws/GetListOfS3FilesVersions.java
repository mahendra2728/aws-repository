package com.pm.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3KeyFilter;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class GetListOfS3FilesVersions {
	
	private static final AWSCredentials credentials;
    private static String bucketName;  

    

    
    static {
        //put your accesskey and secretkey here
        credentials = new BasicAWSCredentials(
          "access-key", 
          "secret-key"
        );
    }

	public static void main(String[] args) {
		
		//set-up the client
        AmazonS3 s3Client = AmazonS3ClientBuilder
          .standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withRegion(Regions.US_EAST_1)
          .build();
		
		ListVersionsRequest request=new ListVersionsRequest()
				.withBucketName("my-bucket2727")
				.withMaxResults(10)
				.withKeyMarker("lastModified");
				
		// 1 2 3 4
		
        VersionListing versionListing = s3Client.listVersions(request);
        
        for (S3VersionSummary objectSummary : versionListing.getVersionSummaries()) {        	
        	System.out.println("Object filename : "+objectSummary.getKey() +
        			"version : "+objectSummary.getVersionId());
    }

	}
}
