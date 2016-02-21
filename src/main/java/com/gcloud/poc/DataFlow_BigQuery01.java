package com.gcloud.poc;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.storage.StorageScopes;

public class DataFlow_BigQuery01 {

    private static final String PROJECT_ID = "engaged-mariner-119119";
    private static final String TABLE_PROJECT_ID = "engaged-mariner-119119";
    private static final String DATASET_ID = "testdb";
    private static final String TABLE_TO_EXPORT = "natality";
    private static final String DESTINATION_URI = "gs://sakthi-poc-bucket/output/natality_output.csv";

    private static final List<String> SCOPES =  Arrays.asList(BigqueryScopes.BIGQUERY);
    private static final HttpTransport TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    public static void main (String[] args) {
        try {
            executeExtractJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final void executeExtractJob() throws IOException, InterruptedException, GeneralSecurityException {
        Bigquery bigquery = createAuthorizedClient();

        //Create a new Extract job
        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationExtract extractConfig = new JobConfigurationExtract();
        TableReference sourceTable = new TableReference();

        sourceTable.setProjectId(TABLE_PROJECT_ID).setDatasetId(DATASET_ID).setTableId(TABLE_TO_EXPORT);
        extractConfig.setSourceTable(sourceTable);
        extractConfig.setDestinationUri(DESTINATION_URI);
        config.setExtract(extractConfig);
        job.setConfiguration(config);

        //Insert/Execute the created extract job
        Insert insert = bigquery.jobs().insert(PROJECT_ID, job);
        insert.setProjectId(PROJECT_ID);
        JobReference jobId = insert.execute().getJobReference();

        //Now check to see if the job has successfuly completed (Optional for extract jobs?)
        long startTime = System.currentTimeMillis();
        long elapsedTime;
        while (true) {
            Job pollJob = bigquery.jobs().get(PROJECT_ID, jobId.getJobId()).execute();
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobId.getJobId(), pollJob.getStatus().getState());
            System.out.println(pollJob.getStatus().getErrorResult());
            if (pollJob.getStatus().getState().equals("DONE")) {
                break;
            }
            //Wait a second before rechecking job status
            Thread.sleep(1000);
        }

    }

    private static Bigquery createAuthorizedClient() throws GeneralSecurityException, IOException {
        GoogleCredential credential = GoogleCredential.getApplicationDefault();
	    if (credential.createScopedRequired()) {
		      credential = credential.createScoped(StorageScopes.all());
		    }
        
        return new Bigquery.Builder(TRANSPORT, JSON_FACTORY , credential)
            .setApplicationName("My Reports")
            .build();
    }
}