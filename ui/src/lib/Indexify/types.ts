export interface IRepository {
  name: string;
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
