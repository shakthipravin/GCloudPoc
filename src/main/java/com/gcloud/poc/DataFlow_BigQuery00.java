
package com.gcloud.poc;

import com.google.api.services.bigquery.Bigquery.Datasets.List;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class DataFlow_BigQuery00 {
  private static final Logger LOG = LoggerFactory.getLogger(DataFlow_BigQuery00.class);
  private static Storage storageService;
  private static final String APPLICATION_NAME = "CLOUD_POC";
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  
  public static void main(String[] args) throws Exception, GeneralSecurityException {
	  Storage client = getService();
	  System.out.println("Starting PipeLine Process!!!");
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  GcpOptions gcpOptions = options.as(GcpOptions.class);
	  gcpOptions.setProject("engaged-mariner-119119");

	  System.out.println("Creating PipeLine Statements!!!");
	  PCollection<TableRow> weatherData = p.apply(
			    BigQueryIO.Read
			         .named("ReadPublicData")
			         .fromQuery("select * from [publicdata:samples.natality] limit 10")
			         .withoutValidation());
	  PCollection formattedResults = weatherData;
	  
	  
	  formattedResults.apply(TextIO.Write.to("gs://sakthi-poc-bucket/output"));


	  /*options.setStagingLocation("gs://sakthi-poc-bucket/staging");
	    
	    p.apply(TextIO.Read.from("gs://sakthi-poc-bucket/shakespeare/*"))
	    .apply(TextIO.Write.to("gs://sakthi-poc-bucket/output")); */
	  System.out.println("Executing PipeLine Statements!!!");
	  p.run();  
	  System.out.println("Process Completed!!!");	  
  }
  private static Storage getService() throws IOException, GeneralSecurityException {
	  if (null == storageService) {
	    GoogleCredential credential = GoogleCredential.getApplicationDefault();
	    // Depending on the environment that provides the default credentials (e.g. Compute Engine,
	    // App Engine), the credentials may require us to specify the scopes we need explicitly.
	    // Check for this case, and inject the Cloud Storage scope if required.
	    if (credential.createScopedRequired()) {
	      credential = credential.createScoped(StorageScopes.all());
	    }
	    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
	    storageService = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
	        .setApplicationName(APPLICATION_NAME).build();
	  }
	  return storageService;
	} 
}
