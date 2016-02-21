package com.gcloud.poc;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
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
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.StorageScopes;

public class DataFlow_BigQuery02 {

    private static final String PROJECT_ID = "engaged-mariner-119119";
    private static final String TABLE_PROJECT_ID = "engaged-mariner-119119";
    private static final String DATASET_ID = "testdb";
    private static final String TABLE_NM = "employee";
    private static final String SOURCE_URI = "gs://sakthi-poc-bucket/input/employee.csv";
    private static final String FILE_DELIM = ",";

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
        JobConfigurationLoad loadConfig = new JobConfigurationLoad();
        
        List<String> sources = new ArrayList<String>();
        sources.add(SOURCE_URI);
        loadConfig.setSourceUris(sources);
        loadConfig.setFieldDelimiter(FILE_DELIM);
        
        
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(TABLE_PROJECT_ID);
        tableRef.setDatasetId(DATASET_ID);
        tableRef.setTableId(TABLE_NM);
        loadConfig.setDestinationTable(tableRef);
        
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        TableFieldSchema emp_id = new TableFieldSchema();
        emp_id.setName("emp_id");
        emp_id.setType("INTEGER");
        emp_id.setMode("REQUIRED");
        
        TableFieldSchema emp_name = new TableFieldSchema();
        emp_name.setName("emp_name");
        emp_name.setType("STRING");
        emp_name.setMode("REQUIRED");
        
        TableFieldSchema emp_type = new TableFieldSchema();
        emp_type.setName("emp_type");
        emp_type.setType("STRING");
        
        TableFieldSchema emp_manager = new TableFieldSchema();
        emp_manager.setName("emp_manager");
        emp_manager.setType("STRING");
        
        TableFieldSchema emp_loc = new TableFieldSchema();
        emp_loc.setName("emp_loc");
        emp_loc.setType("STRING");  
        
        fields.add(emp_id);
        fields.add(emp_name);
        fields.add(emp_type);
        fields.add(emp_manager);
        fields.add(emp_loc);
        
        TableSchema schema = new TableSchema();
        schema.setFields(fields);
        loadConfig.setSchema(schema);
        
        config.setLoad(loadConfig);
        job.setConfiguration(config);
        

        //Insert/Execute the created Load job
        Insert insert = bigquery.jobs().insert(PROJECT_ID, job);
        insert.setProjectId(PROJECT_ID);
        JobReference jobRef =  insert.execute().getJobReference();
        
        long startTime = System.currentTimeMillis();
        long elapsedTime;
        while (true) {
            Job pollJob = bigquery.jobs().get(PROJECT_ID, jobRef.getJobId()).execute();
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobRef.getJobId(), pollJob.getStatus().getState());
            System.out.println(pollJob.getStatus().getErrorResult());
            System.out.println(pollJob.getStatus().getErrors());
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