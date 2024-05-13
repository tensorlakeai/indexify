export interface IExtractionGraphCol {
  displayName: string;
  width: number;
}

export interface IExtractionGraphColumns {
  name: IExtractionGraphCol;
  extractor: IExtractionGraphCol;
  inputParams: IExtractionGraphCol;
  mimeTypes: IExtractionGraphCol;
  taskCount: IExtractionGraphCol;
}
