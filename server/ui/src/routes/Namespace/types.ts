import { IndexifyClient } from 'getindexify'
import {
  Application,
  Namespace,
  ExecutorMetadata,
  ApplicationsList,
  ShallowGraphRequest,
} from '../../types/types'

export interface NamespaceLoaderData {
  namespace: string
  client?: IndexifyClient
}

export interface ApplicationsListLoaderData extends NamespaceLoaderData {
  applications: ApplicationsList
}

export interface IndividualComputeGraphLoaderData extends NamespaceLoaderData {
  invocationsList: ShallowGraphRequest[]
  computeGraph: Application
  prevCursor: string | null
  nextCursor: string | null
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
