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
  [key: string]: {
    tasks: ITask[],
    totalTasks?: {
      unknown?: number,
      success?: number,
      failure?: number,
    },
  };
}

export interface StateChange {
  id: number;
  object_id: string;
  change_type: string;
  created_at: number;
  processed_at: number;
  refcnt_object_id: string | null;
}

export interface ITaskContentMetadata {
  id: string;
  parent_id: string;
  root_content_id: string;
  namespace: string;
  name: string;
  content_type: string;
  labels: Record<string, string>;
  storage_url: string;
  created_at: number;
  source: string;
  size_bytes: number;
  tombstoned: boolean;
  hash: string;
  extraction_policy_ids: Record<string, number>;
}

export interface Row {
    id: number;
    name: string;
    extractor: string;
    inputTypes: string[];
    inputParameters: string;
}
