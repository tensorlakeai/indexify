import { ComputeGraph, ComputeGraphsList, IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import { getIndexifyServiceURL } from './helpers'
import axios from 'axios';

const indexifyServiceURL = getIndexifyServiceURL();

export const apiClient = axios.create({
  baseURL: indexifyServiceURL,
});


function createClient(namespace: string | undefined) {
  if (!namespace) throw new Error('Namespace is required')
  return IndexifyClient.createClient({
    serviceUrl: indexifyServiceURL,
    namespace,
  })
}

export async function ContentsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = createClient(params.namespace)
  return { client }
}

export async function ComputeGraphsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = createClient(params.namespace)
  
  try {
    const computeGraphs = await apiClient.get<ComputeGraphsList>('/namespaces/default/compute_graphs');
    return {
      client,
      computeGraphs: computeGraphs.data,
      namespace: client.namespace,
    }
  } catch (error) {
    console.error("Error fetching compute graphs:", error)
    return {
      client,
      computeGraphs: { compute_graphs: [] },
      namespace: client.namespace,
    }
  }
}

export async function IndividualComputeGraphPageLoader({ params }: LoaderFunctionArgs) {
  const { namespace, 'compute-graph': computeGraph } = params
  if (!namespace) return redirect('/')
  
  try {
    const [computeGraphsResponse, invocationsResponse] = await Promise.all([
      apiClient.get<ComputeGraphsList>('/namespaces/default/compute_graphs'),
      apiClient.get(`/namespaces/default/compute_graphs/${computeGraph}/invocations`)
    ]);

    const localComputeGraph = computeGraphsResponse.data.compute_graphs.find((graph: ComputeGraph) => graph.name === computeGraph);
    
    if (!localComputeGraph) {
      throw new Error(`Compute graph ${computeGraph} not found`);
    }

    return {
      invocationsList: invocationsResponse.data.invocations,
      computeGraph: localComputeGraph,
      namespace,
    }
  } catch (error) {
    console.error("Error fetching compute graph data:", error);
    throw error;
  }
}

export async function InvocationsPageLoader({ params }: LoaderFunctionArgs) {
  const { namespace, 'compute-graph': computeGraph } = params
  if (!namespace) return redirect('/')

  const client = createClient(namespace)
  const invocationsList = await client.getGraphInvocations(computeGraph || '')

  return { namespace, computeGraph, invocationsList }
}

export async function NamespacesPageLoader() {
  const namespaces = (await apiClient.get(`/namespaces`)).data.namespaces;
  return { namespaces }
}

export async function ExecutorsPageLoader() {
  const executors = (await apiClient.get(`/internal/executors`)).data;
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
