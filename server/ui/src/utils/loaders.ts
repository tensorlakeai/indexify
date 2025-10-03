import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import { getIndexifyServiceURL } from './helpers'
import axios from 'axios'
import {
  Application,
  ApplicationsList,
  GraphRequest,
  GraphRequests,
} from '../types/types'

const indexifyServiceURL = getIndexifyServiceURL()

export const apiClient = axios.create({
  baseURL: indexifyServiceURL,
})

async function apiGet<T>(url: string): Promise<T> {
  try {
    const response = await apiClient.get<T>(url)
    return response.data
  } catch (error) {
    console.error(`Error fetching ${url}:`, error)
    throw error
  }
}

function createClient(namespace: string) {
  return IndexifyClient.createClient({
    serviceUrl: indexifyServiceURL,
    namespace: namespace || 'default',
  })
}

export async function ContentsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = createClient(params.namespace)
  return { client }
}

export async function ApplicationsListPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const client = createClient(namespace)

  try {
    const applications = await apiGet<ApplicationsList>(
      `/v1/namespaces/${namespace}/applications`
    )
    return { client, applications, namespace }
  } catch {
    return { client, applications: { applications: [] }, namespace }
  }
}

export async function ApplicationsDetailsPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const application = params.application
  const client = createClient(namespace)

  try {
    const applicationPayload = await apiGet<Application>(
      `/v1/namespaces/${namespace}/applications/${application}`
    )
    const graphRequests = await apiGet<GraphRequests>(
      `/v1/namespaces/${namespace}/applications/${application}/requests?limit=20`
    )
    return {
      client,
      namespace,
      application: applicationPayload,
      graphRequests,
    }
  } catch {
    return { client, namespace, application: null, graphRequests: null }
  }
}

export async function GraphRequestDetailsPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const application = params.application
  const requestId = params['request-id']
  const client = createClient(namespace)

  try {
    const graphRequest = await apiGet<GraphRequest>(
      `/v1/namespaces/${namespace}/applications/${application}/requests/${requestId}`
    )
    return { client, namespace, application, requestId, graphRequest }
  } catch {
    return { client, namespace, application, requestId, graphRequest: null }
  }
}
