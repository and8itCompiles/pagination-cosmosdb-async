package com.test.page;

import java.util.Iterator;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

public class Pagination {

	private static AsyncDocumentClient client;
	private final static String DATABASE_NAME = "test-db";
	private final static String COLLECTION_NAME = "Student";
	private static String hostname = "https://test-db.documents.azure.com:443/";
	private static String accoountKey = "2sQ*****************************************************==";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Connection
		client = new AsyncDocumentClient.Builder().withServiceEndpoint(hostname)
				.withMasterKeyOrResourceToken(accoountKey).withConnectionPolicy(ConnectionPolicy.GetDefault())
				.withConsistencyLevel(ConsistencyLevel.Eventual).build();

		// Method call to retrieve data by pagination
		queryPageByPage();

		// client.close();

	}

	// Method to retrieve data by pagination
	private static void queryPageByPage() {
		// TODO Auto-generated method stub

		int page_size = 10;
		int currentPageNumber = 1;
		int documentNumber = 1;

		String continuationtoken = null;

		do {

			System.out.println("Current Page " + currentPageNumber);
			// /IteratorEnumeration<CeleryTask>

			continuationtoken = executeQuery(page_size, continuationtoken);

			currentPageNumber++;

		} while (continuationtoken != null);

	}

	private static String executeQuery(int pageSize, String continuationToken) {
		// TODO Auto-generated method stub

		FeedOptions queryOptions = new FeedOptions();
		// queryOptions.setPageSize(-1);
		queryOptions.setEnableCrossPartitionQuery(false);

		queryOptions.setMaxItemCount(pageSize);
		queryOptions.setRequestContinuation(continuationToken);

		String collectionLink = String.format("/dbs/%s/colls/%s", DATABASE_NAME, COLLECTION_NAME);

		Iterator<FeedResponse<Document>> it = client.queryDocuments(collectionLink,
				"Select * from Student",
				queryOptions).toBlocking().getIterator();

		System.out.println("Running SQL query...");
		FeedResponse<Document> page = null;

		if (it.hasNext()) {
			page = it.next();
			System.out.println(page.getResponseContinuation());

			for (Document doc : page.getResults()) {
				System.out.println(String.format("\t doc %s", doc));
			}
		}

		return page.getResponseContinuation();

	}

}
