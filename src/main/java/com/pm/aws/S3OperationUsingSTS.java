package com.pm.aws;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class S3OperationUsingSTS {

	private static final AWSCredentials credentials;

	static {
		credentials = new BasicAWSCredentials(
		          "access-key", 
		          "secret-key"
		        );	}

	public static void main(String[] args) {

		final String RoleArn = "ASSUME_ROLE_ARN";
		final String SessionName = "MySession1";

		AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.AP_SOUTH_1).build();

		AssumeRoleRequest request = new AssumeRoleRequest().withRoleArn(RoleArn).withRoleSessionName(SessionName)
				.withDurationSeconds(3000);

		AssumeRoleResult result = stsClient.assumeRole(request);

		Credentials tempCredentials = result.getCredentials();

		System.out.println("Access Key : " + tempCredentials.getAccessKeyId());
		System.out.println("Secret Access Key : " + tempCredentials.getSecretAccessKey());
		// System.out.println("Token : " + tempCredentials.getSessionToken());

		BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(tempCredentials.getAccessKeyId(),
				tempCredentials.getSecretAccessKey(), tempCredentials.getSessionToken());

		// perform S3 operation
		performOperationsOnS3(sessionCredentials);

		// DynamoDb
		// dynamoDbOperations(sessionCredentials);

	}

	public static void performOperationsOnS3(BasicSessionCredentials sessionCredentials) {
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(sessionCredentials)).withRegion(Regions.US_EAST_1)
				.build();

		ListVersionsRequest request = new ListVersionsRequest().withBucketName("my-bucket2727").withMaxResults(10);

		// 1 2 3 4

		VersionListing versionListing = s3.listVersions(request);

		List<S3Object> objectList = new ArrayList<S3Object>();
		Map<String, String> map = new LinkedHashMap<String, String>();
		List<S3Object> objectS3List = new ArrayList<S3Object>();

		for (S3VersionSummary objectSummary : versionListing.getVersionSummaries()) {

			if (map.containsKey(objectSummary.getKey() + "@" + objectSummary.getETag())) {
				map.put(objectSummary.getKey() + "@" + objectSummary.getETag(),
						map.get(objectSummary.getKey() + "@" + objectSummary.getETag()) + ","
								+ objectSummary.getVersionId());
			} else {
				map.put(objectSummary.getKey() + "@" + objectSummary.getETag(), objectSummary.getVersionId());
			}

			S3Object object = s3.getObject(new GetObjectRequest("my-bucket2727", objectSummary.getKey()));
			System.out.println(
					"Object filename : " + objectSummary.getKey() + " version : " + objectSummary.getVersionId());

			objectS3List.add(object);

		}
		System.out.println(map);
		System.out.println("Done");
		// List out Buckets presents
//		List<Bucket> buckets = s3.listBuckets();
//		for (Bucket b : buckets) {
//			System.out.println("Bucket Found : " + b.getName());
//		}

		// Create Bucket
		/*
		 * Bucket b = s3.createBucket("test272827"); if (b != null) {
		 * System.out.println("Bucket Created"); }
		 */

		// delete Bucket
		// s3.deleteBucket(b.getName());
		// System.out.println("Deleted Bucket : " + b.getName());
	}

	public static void dynamoDbOperations(BasicSessionCredentials sessionCredentials) {

		// https://dynamodb.us-east-1.amazonaws.com

//		DynamoDbClient client = DynamoDbClient.builder()
//			    .endpointOverride(URI.create("https://dynamodb.us-east-1.amazonaws.com"))
//			    .region(software.amazon.awssdk.regions.Region.US_EAST_1)
//			    .credentialsProvider(StaticCredentialsProvider.create(
//			    AwsBasicCredentials.create(sessionCredentials.getAWSAccessKeyId(), sessionCredentials.getAWSSecretKey())))
//			    .build();

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new EndpointConfiguration("https://dynamodb.us-east-1.amazonaws.com", "us-east-1")).build();
		DynamoDB db = new DynamoDB(client);
		String tableName = "students";
		try {

			CreateTableRequest request = new CreateTableRequest()
					.withAttributeDefinitions(new AttributeDefinition("first_name", ScalarAttributeType.S))
					.withKeySchema(new KeySchemaElement("student_id", KeyType.HASH))
					.withProvisionedThroughput(new ProvisionedThroughput(new Long(1), new Long(1)))
					.withTableName("students");

			Table table = db.createTable(request);

//
//		 Table table=client.createTable(request);

//            System.out.println("Attempting to create table; please wait...");
//            Table table = dynamoDB.createTable(tableName,
//                Arrays.asList(new KeySchemaElement("student_id", KeyType.HASH)), // Partition Key  
//                Arrays.asList(new AttributeDefinition(),new AttributeDefinition()),
//                new ProvisionedThroughput(1L, 1L));
			table.waitForActive();
			System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

		} catch (Exception e) {
			System.err.println("Unable to create table: ");
			System.err.println(e.getMessage());
		}

	}

	public static Map<String, List<String>> maintainVersions(String key, String versionId) {
		Map<String, List<String>> map = new LinkedHashMap<String, List<String>>();
		return map;
	}
}
