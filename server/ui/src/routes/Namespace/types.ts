import { IndexifyClient } from 'getindexify'
import {
  ComputeGraph,
  Namespace,
  ExecutorMetadata,
  ComputeGraphsList,
  Invocation,
} from '../../types'

export interface NamespaceLoaderData {
  namespace: string
  client?: IndexifyClient
}

export interface ComputeGraphLoaderData extends NamespaceLoaderData {
  computeGraphs: ComputeGraphsList
}

export interface IndividualComputeGraphLoaderData extends NamespaceLoaderData {
  invocationsList: Invocation[]
  computeGraph: ComputeGraph
  cursor: string | null;
  direction?: string
}

export interface IndividualInvocationLoaderData extends NamespaceLoaderData {
  indexifyServiceURL: string
  invocationId: string
  computeGraph: string
}

export interface ExecutorsLoaderData {
  executors: ExecutorMetadata[]
}

export interface NamespacesLoaderData {
  namespaces: Namespace[]
}
