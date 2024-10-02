import { ComputeGraph, ComputeGraphsList, IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  getIndexifyServiceURL,
} from './helpers'
import axios from 'axios';

const apiClient = axios.create({
  baseURL: getIndexifyServiceURL(),
});

function createClient(namespace: string | undefined) {
  if (!namespace) throw new Error('Namespace is required')
  return IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })
}

export async function ContentsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = createClient(params.namespace)
  return { client }
}

export async function ComputeGraphsPageLoader({
  params,
}: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = createClient(params.namespace)
  
  try {
    const computeGraphs = await apiClient.get<ComputeGraphsList>('/namespaces/default/compute_graphs');
    return {
      client: client,
      computeGraphs: computeGraphs.data,
      namespace: client.namespace,
    }
  } catch (error) {
    console.error("Error fetching compute graphs:", error)
    return {
      client: client,
      computeGraphs: { compute_graphs: [] },
      namespace: client.namespace,
    }
  }
}

export async function IndividualComputeGraphPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace } = params
  const computeGraph = params['compute-graph']
  if (!namespace) return redirect('/')
  
  const computeGraphs = (await apiClient.get<ComputeGraphsList>('/namespaces/default/compute_graphs')).data;

  const localComputeGraph = computeGraphs.compute_graphs.find((graph: ComputeGraph) => graph.name === computeGraph);
  
  const invocationsList = (await apiClient.get(`/namespaces/default/compute_graphs/${computeGraph}/invocations`)).data.invocations;
  if (!computeGraph) {
    throw new Error(`Extraction graph ${localComputeGraph} not found`);
  }

  return {
    invocationsList,
    computeGraph: localComputeGraph,
    namespace: params.namespace,
  }
}

export async function InvocationsPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace } = params
  const computeGraph = params['compute-graph']
  if (!namespace) return redirect('/')

  const client = createClient(namespace)

  const invocationsList = await client.getGraphInvocations(computeGraph ? computeGraph : '')

  return { namespace, computeGraph, invocationsList }
}

export async function NamespacesPageLoader() {
  const namespaces = await IndexifyClient.namespaces()
  return { namespaces }
}

export async function ExecutorsPageLoader() {
  const executors = (await apiClient.get(`/internal/executors`)).data;
  
  return { executors }
}

export async function IndividualInvocationPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const { namespace } = params
  const computeGraph = params['compute-graph']
  const invocationId = params['invocation-id']
  const indexifyServiceURL = getIndexifyServiceURL();

  return {
    indexifyServiceURL,
    invocationId,
    computeGraph,
    namespace: namespace,
  }
}