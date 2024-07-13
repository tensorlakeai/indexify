import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  getExtractionPolicyTaskCounts,
  getIndexifyServiceURL,
} from './helpers'
import { IHash, TaskCounts } from '../types'

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
  const extractors = await client.extractors()
  const extractionGraphs = await client.getExtractionGraphs();

  return {
    namespace: client.namespace,
    extractionGraphs: extractionGraphs,
    extractors,
  }
}

export async function IndividualExtractionGraphPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, extraction_graph } = params
  const extractorName = extraction_graph
  if (!namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const extractionGraphs = await client.getExtractionGraphs()

  const extractionGraph = extractionGraphs.filter(extractionGraph => extractionGraph.name === extractorName)[0];
  const contentList = await client.listContent(extractionGraph.name);
  const [extractors] = await Promise.all([
    client.extractors(),
  ])
  let tasks_by_policies: IHash = {}
  extractionGraph.extraction_policies.forEach(async (extractionPolicy) => {
    tasks_by_policies[extractionPolicy.name] = await client.getTasks(extractionGraph.name, extractionPolicy.name)
  })
  return {
    tasks: tasks_by_policies,
    extractors,
    extractionGraph: extractionGraph,
    contentList,
    namespace: params.namespace,
    extractorName
  }
}

export async function ExtractionPolicyPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, policyname, graphname } = params
  if (!namespace || !policyname) return redirect('/')

  const client = await createClient(namespace)
  const extractionGraphs = await client.getExtractionGraphs()
  const extractionGraph = extractionGraphs.find(
    (graph) => graph.name === graphname
  )
  const policy = extractionGraphs
    .map((graph) => graph.extraction_policies)
    .flat()
    .find(
      (policy) => policy.name === policyname && policy.graph_name === graphname
    )
  
  let taskCounts:TaskCounts | undefined = undefined
  if (policy?.id) {
    taskCounts = await getExtractionPolicyTaskCounts(graphname!, policy.name, namespace, client )
  }
    
  return { policy, namespace, extractionGraph, client, taskCounts }
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
  const extractionGraphs = await client.getExtractionGraphs();
  const indexes = await client.indexes()
  return {
    indexes,
    namespace: params.namespace,
    extractionGraphs: extractionGraphs,
  }
}

export async function IndividualContentPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, extractorName, contentId } = params
  if (!namespace || !contentId) return redirect('/')

  const client = await createClient(namespace)
  const extractionGraphs = await client.getExtractionGraphs();

  const contentMetadata = await client.getContentMetadata(contentId)

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
  const indexes = (await client.indexes()).filter(
    (index) => index.name === indexName
  )
  if (!indexes.length) {
    return redirect('/')
  }

  return { index: indexes[0], namespace, client }
}

export async function SqlTablesPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const schemas = await client.getSchemas()
  return { schemas }
}
