import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  getIndexifyServiceURL,
} from './helpers'
import axios from 'axios'

async function createClient(namespace: string | undefined) {
  if (!namespace) throw new Error('Namespace is required')
  return await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })
}

export async function ContentsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  return { client }
}

export async function ExtractionGraphsPageLoader({
  params,
}: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)

  return {
    client: client,
    namespace: client.namespace,
  }
}

export async function IndividualExtractionGraphPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, extraction_graph } = params
  const extractorName = extraction_graph
  if (!namespace) return redirect('/')
  
  const client = await createClient(params.namespace)
  
  const [extractionGraphs, extractors] = await Promise.all([
    client.getExtractionGraphs(),
    client.extractors(),
  ])

  const extractionGraph = extractionGraphs.find(graph => graph.name === extractorName);
  if (!extractionGraph) {
    throw new Error(`Extraction graph ${extractorName} not found`);
  }

  return {
    extractors,
    extractionGraph,
    client,
    namespace: params.namespace,
    extractorName
  }
}

export async function ExtractionPolicyPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, policyName, extraction_graph } = params
  if (!namespace || !policyName) return redirect('/')

  const client = await createClient(namespace)
  const [extractionGraphs] = await Promise.all([
    client.getExtractionGraphs()
  ])

  const extractionGraph = extractionGraphs.find(
    (graph) => graph.name === extraction_graph
  )
  const policy = extractionGraphs
    .flatMap((graph) => graph.extraction_policies)
    .find(
      (policy) => policy.name === policyName && policy.graph_name === extraction_graph
    )

  return { policy, namespace, extractionGraph, client }
}

export async function ExtractorsPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const extractors = await client.extractors()
  return { extractors }
}

export async function IndexesPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const indexes = await client.indexes()
  return {
    indexes,
    namespace: params.namespace,
  }
}

export async function IndividualContentPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, extractorName, contentId } = params
  if (!namespace || !contentId) return redirect('/')

  const client = await createClient(namespace)
  const [extractionGraphs, contentMetadata] = await Promise.all([
    client.getExtractionGraphs(),
    client.getContentMetadata(contentId)
  ])

  return {
    client,
    namespace,
    contentId,
    contentMetadata,
    extractorName,
    extractionGraphs
  }
}

export async function NamespacePageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const [extractors, indexes, schemas] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getSchemas(),
  ])
  return {
    client,
    extractors,
    indexes,
    schemas,
    namespace: params.namespace,
  }
}

export async function SearchIndexPageLoader({ params }: LoaderFunctionArgs) {
  const { namespace, indexName } = params
  if (!namespace || !indexName) return redirect('/')

  const client = await createClient(namespace)
  const indexes = await client.indexes()
  const index = indexes.find((index) => index.name === indexName)
  if (!index) {
    return redirect('/')
  }

  return { index, namespace, client }
}

export async function SqlTablesPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const schemas = await client.getSchemas()
  return { schemas }
}

export async function StateChangesPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const response = await axios.get(`${getIndexifyServiceURL()}/state_changes`);
  const stateChanges = response.data.state_changes
  return { stateChanges };
}
