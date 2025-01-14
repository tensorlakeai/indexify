## Dataset 
Abstraction to create indexes from a collection of documents by specifying -
1. Parsing Strategy 
2. Embedding Models
3. Database URL

The dataset can be updated continuously by the user, when there are new documents and the indexes get updated by Tensorlake.

1. Create a Dataset in your project
2. Add Documents to the dataset with some parsing scheme 
3. The dataset gets updated 
4. The dataset is written to your database


Application Flow is -
1. Read from the database during retrieval. 

## What are Datasets 
1. A dataset is a collection of related documents.
2. Datasets can be used to automatically parse, chunk, and embed documents for retrieval by LLM applications.
3. Datasets can be continously updated asynchronously by data ingestion pipelines.
4. Datasets can be exported into databases automatically or using APIs.


Two different modalities -
1. Out of the box Datasets abstraction for retrieval from documents.
2. Custom workflows for building end to end pipelines for document understanding use cases with complete control over the transformations and extraction process.

## Approach 2 - Index 
1. Crate an Index, specify the parsing mechanism and the embedding models and indexing schema.
2. Add new documents to the index. 


Sarma - 
1. Each dataset is a collection of documents. 
2. Each document has a document id. 
3. Each document also has a pointer to workflow outputs. This is represented as K/V, document id -> output in workflow.
4. The user can query and check if the dataset has been extracted, transformed and ingested into an index.

The utility of the dataset abstraction is to check whether a document has made it all the way to an index. 

Datasets needs to provide a `status` API, and provide information about -
1. Total number of documents. 
2. Current state/marker of ingestion.


## Define the problem of creating indexes for retrieval.
## The general shape of pipelines for building such indexes.
## An out of the box solution as an API
## Levers of the API 
### Define the transformations that can be attached to a Dataset
### Talk about the API to ingest into Datasets
### Current Status of the Dataset



## Getting even more flexibility - Write serverless workflows yourself. 
1. You can write abstractions such as Datasets yourself on our platform. We have even opensourced the workflow code we use for the Datasets abstrction. You can tweak it to implement new algorithms and deploy this as a service in your account as an API.