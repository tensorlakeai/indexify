import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  getExtractionPolicyTaskCounts,
  getIndexifyServiceURL,
} from './helpers'
import { IHash, TaskCounts } from '../types'
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
  const [extractors] = await Promise.all([
    client.extractors(),
  ])

  const tasksByPolicies: IHash = {};

  for (const extractionPolicy of extractionGraph.extraction_policies) {
    tasksByPolicies[extractionPolicy.name] = await client.getTasks(
      extractionGraph.name,
      extractionPolicy.name,
    );
  }
  return {
    tasks: tasksByPolicies,
    extractors,
    extractionGraph: extractionGraph,
    client,
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
    taskCounts = await getExtractionPolicyTaskCounts(graphname!, policy.name, client )
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

export async function StateChangesPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const response = await axios.get(`${getIndexifyServiceURL()}/state_changes`);
  const stateChanges = response.data.state_changes
    return { stateChanges };
}

export async function ExtractionPoliciesContentPageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const { namespace, extractorName } = params
  if (!params.namespace) return redirect('/')

  const client = await createClient(params.namespace)
  const extractionGraphs = await client.getExtractionGraphs();
  const extractionGraph = extractionGraphs.find(
    (graph) => graph.name === extractorName
  )

  return {
    client,
    extractorName,
    extractionGraph,
    extractionGraphs,
    namespace
  }
}


