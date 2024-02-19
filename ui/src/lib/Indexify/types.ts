export interface INamespace {
  name: string;
  extraction_policies: IExtractionPolicy[];
}

export interface IEmbeddingSchema {
  distance: string;
  dim: number;
}

export interface IExtractorSchema {
  outputs: { [key: string]: IEmbeddingSchema | { [key: string]: any } };
}

export interface IExtractor {
  name: string;
  description: string;
  input_params: { [key: string]: any };
  outputs: IExtractorSchema;
}

export interface IIndex {
  name: string;
  schema: Record<string, string | number>;
}

export interface IContentMetadata {
  content_type: string;
  created_at: number;
  id: string;
  labels: Record<string, string>;
  name: string;
  parent_id?: string;
  namespace: string;
  source: string;
  storage_url: string;
}

export interface IContent {
  bytes: number[];
  content_type: string;
}

export interface IExtractionPolicy {
  content_source: string;
  extractor: string;
  filters_eq: Record<string, string>;
  input_params: { [key: string]: any };
  name: string;
}

export interface ITask {
  content_metadata: IContentMetadata;
  extractor: string;
  extraction_policy: string;
  id: string;
  input_params: Record<string, string>;
  outcome: string;
  output_index_table_mapping: {
    embedding: string;
  };
  repository: string;
}
