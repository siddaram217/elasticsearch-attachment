# elasticsearch-attachment
Simple Java Example to index  file document in elasticsearch


Step1: Add 'ingest-attachment' plugin to elasticsearch follow below url steps.
	Url: https://www.elastic.co/guide/en/elasticsearch/plugins/current/ingest-attachment.html

Step2: create attachment process for attachment type data 
 
	 PUT _ingest/pipeline/attachment
	{
	  "description" : "Extract attachment information",
	  "processors" : [
		{
		  "attachment" : {
			"field" : "data"
		  }
		}
	  ]
	}
 
Step3: Create index (with any name)
 
	PUT attachments
