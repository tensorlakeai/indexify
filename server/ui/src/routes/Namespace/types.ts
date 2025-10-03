import { IndexifyClient } from 'getindexify'
import {
  Application,
  ApplicationsList,
  ExecutorMetadata,
  GraphRequests,
  Namespace,
} from '../../types/types'

export interface NamespaceLoaderData {
  namespace: string
  client?: IndexifyClient
}

export interface ApplicationsListLoaderData extends NamespaceLoaderData {
  applications: ApplicationsList
}

export interface ApplicationDetailsLoaderData extends NamespaceLoaderData {
  application: Application
  graphRequests: GraphRequests
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
