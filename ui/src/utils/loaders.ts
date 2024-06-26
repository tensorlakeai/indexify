import { isAxiosError } from 'axios'
import { IndexifyClient, IExtractedMetadata } from 'getindexify'
import { LoaderFunctionArgs, redirect } from 'react-router-dom'
import {
  getExtractionPolicyTaskCounts,
  getIndexifyServiceURL,
  groupMetadataByExtractor,
} from './helpers'
import { TaskCounts, TaskCountsMap } from '../types'

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

  const counts = await Promise.all(
    client.extractionGraphs.map(async (graph) => {
      return Promise.all(
        graph.extraction_policies
          .map(async (policy) => {
            if (!policy.id) return null
            return {
              id: policy.id,
              counts: await getExtractionPolicyTaskCounts(policy.id, client),
            }
          })
          .filter((result) => result !== null)
      )
    })
  )
  const countsArray = counts.flat()
  const taskCountsMap: TaskCountsMap = new Map<
    string,
    { totalSuccess: number; totalFailed: number; totalUnknown: number }
  >()
  countsArray.forEach((policyCounts) => {
    if (policyCounts && policyCounts.id) {
      taskCountsMap.set(policyCounts.id, policyCounts.counts)
    }
  })

  return {
    namespace: client.namespace,
    extractionGraphs: client.extractionGraphs,
    extractors,
    taskCountsMap,
  }
}

export async function ExtractionPolicyPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, policyname, graphname } = params
  if (!namespace || !policyname) return redirect('/')

  const client = await createClient(namespace)
  const extractionGraph = client.extractionGraphs.find(
    (graph) => graph.name === graphname
  )
  const policy = client.extractionGraphs
    .map((graph) => graph.extraction_policies)
    .flat()
    .find(
      (policy) => policy.name === policyname && policy.graph_name === graphname
    )
  
  let taskCounts:TaskCounts | undefined = undefined
  if (policy?.id) {
    taskCounts = await getExtractionPolicyTaskCounts(policy.id, client)
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
  const indexes = await client.indexes()
  return {
    indexes,
    namespace: params.namespace,
    extractionGraphs: client.extractionGraphs,
  }
}

export async function IndividualContentPageLoader({
  params,
}: LoaderFunctionArgs) {
  const { namespace, contentId } = params
  if (!namespace || !contentId) return redirect('/')

  const client = await createClient(namespace)
  const errors: string[] = []

  const tasks = await client
    .getTasks({ returnTotal: false })
    .then(({ tasks }) =>
      tasks.filter((t) => t.content_metadata.id === contentId)
    )
    .catch((e) => {
      if (isAxiosError(e)) {
        errors.push(`getTasks: ${e.message}`)
      }
      return null
    })

  const contentMetadata = await client.getContentMetadata(contentId)
  const extractedMetadataList = await client
    .getStructuredMetadata(contentId)
    .catch((e) => {
      if (isAxiosError(e)) {
        errors.push(
          `getExtractedMetadata: ${e.message} - ${
            e.response?.data || 'unknown'
          }`
        )
      }
      return [] as IExtractedMetadata[]
    })

  return {
    client,
    namespace,
    tasks,
    contentId,
    contentMetadata,
    groupedExtractedMetadata: groupMetadataByExtractor(extractedMetadataList),
    errors,
  }
}

export async function NamespacePageLoader({ params }: LoaderFunctionArgs) {
  if (!params.namespace) return redirect('/')
  const client = await createClient(params.namespace)
  const [extractors, indexes, contentList, schemas] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getExtractedContent(),
    client.getSchemas(),
  ])
  return {
    client,
    extractors,
    indexes,
    contentList,
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
