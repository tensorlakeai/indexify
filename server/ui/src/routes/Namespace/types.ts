import { IndexifyClient } from 'getindexify';
import { ComputeGraph, DataObject, Namespace, ExecutorMetadata, ComputeGraphsList } from '../../types';

export interface NamespaceLoaderData {
  namespace: string;
  client?: IndexifyClient;
}

export interface ComputeGraphLoaderData extends NamespaceLoaderData {
  computeGraphs: ComputeGraphsList;
}

export interface IndividualComputeGraphLoaderData extends NamespaceLoaderData {
  invocationsList: DataObject[];
  computeGraph: ComputeGraph;
}

export interface IndividualInvocationLoaderData extends NamespaceLoaderData {
  indexifyServiceURL: string;
  invocationId: string;
  computeGraph: string;
}

export interface ExecutorsLoaderData {
  executors: ExecutorMetadata[];
}

export interface NamespacesLoaderData {
  namespaces: Namespace[];
}
