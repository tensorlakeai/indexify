import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import { getIndexifyServiceURL } from './helpers'
import axios from 'axios'
import { ComputeGraph, ComputeGraphsList } from '../types'

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

export async function ComputeGraphsPageLoader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default'
  const client = createClient(namespace)
  
  try {
    const computeGraphs = await apiGet<ComputeGraphsList>(`/namespaces/${namespace}/compute_graphs`)
    return { client, computeGraphs, namespace }
  } catch {
    return { client, computeGraphs: { compute_graphs: [] }, namespace }
  }
}

export async function IndividualComputeGraphPageLoader({ params }: LoaderFunctionArgs) {
  const { namespace, 'compute-graph': computeGraph } = params
  if (!namespace) return redirect('/')
  
  try {
    const [computeGraphs, invocations] = await Promise.all([
      apiGet<ComputeGraphsList>(`/namespaces/${namespace}/compute_graphs`),
      apiGet<{ invocations: unknown[] }>(`/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations`)
    ])

    const localComputeGraph = computeGraphs.compute_graphs.find(
      (graph: ComputeGraph) => graph.name === computeGraph
    )
    
    if (!localComputeGraph) {
      throw new Error(`Compute graph ${computeGraph} not found`)
    }

    return {
      invocationsList: invocations.invocations,
      computeGraph: localComputeGraph,
      namespace,
    }
  } catch (error) {
    console.error("Error fetching compute graph data:", error)
    throw error
  }
}

export async function InvocationsPageLoader({ params }: LoaderFunctionArgs) {
  const { namespace, 'compute-graph': computeGraph } = params
  if (!namespace) return redirect('/')

  const client = createClient(namespace)
  const invocationsList = await client.getGraphInvocations(computeGraph || '')

  return { namespace, computeGraph, invocationsList }
}

export async function ExecutorsPageLoader() {
  const executors = await apiGet<unknown>('/internal/executors')
  return { executors }
}

export async function IndividualInvocationPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const { namespace, 'compute-graph': computeGraph, 'invocation-id': invocationId } = params

  return {
    indexifyServiceURL,
    invocationId,
    computeGraph,
    namespace,
  }
}
