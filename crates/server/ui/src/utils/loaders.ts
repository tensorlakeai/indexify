import axios from 'axios'
import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  Application,
  ApplicationRequests,
  ApplicationsList,
  ListSandboxesResponse,
  SandboxInfo,
} from '../types/types'
import { getIndexifyServiceURL } from './helpers'

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

export async function ExecutorsPageLoader() {
  const executors = await apiGet<unknown>('/internal/executors')
  return { executors }
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
    const applicationRequests = await apiGet<ApplicationRequests>(
      `/v1/namespaces/${namespace}/applications/${application}/requests?limit=20`
    )
    // Fetch sandboxes for this namespace (sandboxes are now at namespace level)
    let sandboxes: ListSandboxesResponse = { sandboxes: [] }
    try {
      sandboxes = await apiGet<ListSandboxesResponse>(
        `/v1/namespaces/${namespace}/sandboxes`
      )
    } catch {
      // Sandboxes endpoint may not exist or return empty
    }
    return {
      client,
      namespace,
      application: applicationPayload,
      applicationRequests,
      sandboxes,
    }
  } catch {
    return { client, namespace, application: null, applicationRequests: null, sandboxes: { sandboxes: [] } }
  }
}

export async function ApplicationRequestDetailsPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const application = params.application
  const requestId = params['request-id']
  const client = createClient(namespace)

  try {
    const applicationRequest = await apiGet<Request>(
      `/v1/namespaces/${namespace}/applications/${application}/requests/${requestId}`
    )
    return { client, namespace, application, requestId, applicationRequest }
  } catch {
    return {
      client,
      namespace,
      application,
      requestId,
      applicationRequest: null,
    }
  }
}

export async function SandboxesListPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const client = createClient(namespace)

  try {
    const sandboxes = await apiGet<ListSandboxesResponse>(
      `/v1/namespaces/${namespace}/sandboxes`
    )
    return { client, sandboxes, namespace }
  } catch {
    return { client, sandboxes: { sandboxes: [] }, namespace }
  }
}

export async function SandboxDetailsPageLoader({
  params,
}: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const sandboxId = params['sandbox-id']
  const client = createClient(namespace)

  try {
    const sandbox = await apiGet<SandboxInfo>(
      `/v1/namespaces/${namespace}/sandboxes/${sandboxId}`
    )
    return { client, namespace, sandboxId, sandbox }
  } catch {
    return {
      client,
      namespace,
      sandboxId,
      sandbox: null,
    }
  }
}
