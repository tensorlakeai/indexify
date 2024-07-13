import { IContentMetadata, ITask } from 'getindexify'

export interface IExtractionGraphCol {
  displayName: string
  width: number
}

export interface IExtractionGraphColumns {
  name: IExtractionGraphCol
  extractor: IExtractionGraphCol
  inputParams: IExtractionGraphCol
  mimeTypes: IExtractionGraphCol
  taskCount?: IExtractionGraphCol
}

export interface IContentMetadataExtended extends IContentMetadata {
  children?: number
}

export interface TaskCounts {
  totalSuccess: number
  totalFailed: number
  totalUnknown: number
}

export type TaskCountsMap = Map<
  string,
  TaskCounts
>

export interface IHash {
  [key: string]: ITask[];
}
