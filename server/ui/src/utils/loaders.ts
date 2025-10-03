import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import { getIndexifyServiceURL } from './helpers'
import axios from 'axios'
import { Application, ApplicationsList } from '../types/types'

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

// export async function IndividualComputeGraphPageLoader({
//   params,
//   request,
// }: LoaderFunctionArgs) {
//   const { namespace, application, request_id } = params
//   if (!namespace) return redirect('/')

//   const url = new URL(request.url)
//   const cursor = url.searchParams.get('cursor') || undefined
//   const direction = url.searchParams.get('direction') || 'forward'
//   const limit = 20

//   try {
//     const requestsUrl = `/v1/namespaces/${namespace}/applications/${application}/requests/${request_id}/function-runs?limit=${limit}${
//       cursor ? `&cursor=${cursor}` : ''
//     }${direction ? `&direction=${direction}` : ''}`

//     const [computeGraphs, requestsResponse] = await Promise.all([
//       apiGet<ApplicationsList>(`/v1/namespaces/${namespace}/applications`),
//       apiGet<{
//         invocations: unknown[]
//         prev_cursor?: string
//         next_cursor?: string
//       }>(requestsUrl),
//     ])

//     const localComputeGraph = computeGraphs.applications.find(
//       (graph: Application) => graph.name === computeGraph
//     )

//     if (!localComputeGraph) {
//       throw new Error(`Compute graph ${computeGraph} not found`)
//     }

//     return {
//       invocationsList: invocationsResponse.invocations,
//       prevCursor: invocationsResponse.prev_cursor,
//       nextCursor: invocationsResponse.next_cursor,
//       currentDirection: direction,
//       computeGraph: localComputeGraph,
//       namespace,
//     }
//   } catch (error) {
//     console.error('Error fetching compute graph data:', error)
//     throw error
//   }
// }

// export async function InvocationsPageLoader({ params }: LoaderFunctionArgs) {
//   const { namespace, 'compute-graph': computeGraph } = params
//   if (!namespace) return redirect('/')

//   const client = createClient(namespace)
//   const invocationsList = await client.getGraphInvocations(computeGraph || '')

//   return { namespace, computeGraph, invocationsList }
// }

// export async function ExecutorsPageLoader() {
//   const executors = await apiGet<unknown>('/internal/executors')
//   return { executors }
// }

// export async function IndividualInvocationPageLoader({
//   params,
// }: LoaderFunctionArgs) {
//   if (!params.namespace) return redirect('/')
//   const {
//     namespace,
//     'compute-graph': computeGraph,
//     'invocation-id': invocationId,
//   } = params

//   return {
//     indexifyServiceURL,
//     invocationId,
//     computeGraph,
//     namespace,
//   }
// }
