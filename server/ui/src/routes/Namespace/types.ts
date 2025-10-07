import { IndexifyClient } from 'getindexify'
import {
  Application,
  ApplicationRequests,
  ApplicationsList,
  ExecutorMetadata,
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
  applicationRequests: ApplicationRequests
}

export interface ExecutorsLoaderData {
  executors: ExecutorMetadata[]
}

export interface NamespacesLoaderData {
  namespaces: Namespace[]
}
