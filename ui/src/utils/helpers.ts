import { ExtractionGraph, Extractor, IExtractedMetadata, IndexifyClient } from 'getindexify'
import { IHash, Row } from '../types'

export const stringToColor = (str: string): string => {
  let hash = 5381
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) + str.charCodeAt(i)
  }
  return `#${(hash & 0xFFFFFF).toString(16).padStart(6, '0')}`
}


export const groupMetadataByExtractor = (
  metadataArray: IExtractedMetadata[]
): Map<string, IExtractedMetadata[]> => {
  return metadataArray.reduce((accumulator, currentItem) => {
    const key = currentItem.extractor_name
    if (!accumulator.has(key)) {
      accumulator.set(key, [])
    }
    accumulator.get(key)!.push(currentItem)
    return accumulator
  }, new Map<string, IExtractedMetadata[]>())
}


export const getIndexifyServiceURL = (): string => {
  return process.env.NODE_ENV === 'development' ? 'http://localhost:8900' : window.location.origin
}


export const formatBytes = (() => {
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const k = 1000
  return (bytes: number, decimals: number = 2): string => {
    if (bytes === 0) return '0 Bytes'
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`
  }
})()


export const getExtractionPolicyTaskCounts = async (
  extractionGraph: string,
  extractionPolicyName: string,
  client: IndexifyClient
): Promise<any> => {
  return client.getTasks(extractionGraph, extractionPolicyName)
}


export const splitLabels = (data: { [key: string]: string }): string[] => {
  return Object.entries(data).map(([key, value]) => `${key}: ${value}`)
}


export const mapExtractionPoliciesToRows = (
  extractionGraph: ExtractionGraph | ExtractionGraph[],
  extractors: Extractor[],
  graphName: string
): Row[] => {
  const extractorMap = new Map(extractors.map(e => [e.name, e]))
  
  const targetGraph = Array.isArray(extractionGraph)
    ? extractionGraph.find(graph => graph.name === graphName)
    : extractionGraph.name === graphName ? extractionGraph : undefined
  
  if (!targetGraph) {
    console.error(`No graph found with name: ${graphName}`)
    return []
  }
  
  return targetGraph.extraction_policies.map((policy, index) => {
    const extractor = extractorMap.get(policy.extractor)
    
    return {
      id: index + 1,
      name: policy.name,
      extractor: policy.extractor,
      inputTypes: extractor ? extractor.input_mime_types : ['Unknown'],
      inputParameters: policy.input_params ? JSON.stringify(policy.input_params) : 'None',
    }
  })
}

export async function getTasksForExtractionGraph(
  extractionGraph: ExtractionGraph,
  client: any,
): Promise<IHash> {
  const tasks_by_policies: IHash = {}

  for (const extractionPolicy of extractionGraph.extraction_policies) {
    tasks_by_policies[extractionPolicy.name] = await client.getTasks(
      extractionGraph.name,
      extractionPolicy.name
    )
  }
  return tasks_by_policies
}

export const formatTimestamp = (() => {
  const MILLISECONDS_MULTIPLIER = 1e12
  const dateFormatOptions: Intl.DateTimeFormatOptions = {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  }

  return (value: string | number | null | undefined): string => {
    if (value == null) return 'N/A'
    
    const timestamp = typeof value === 'string' ? parseInt(value, 10) : value
    
    if (typeof timestamp !== 'number' || isNaN(timestamp)) return 'Invalid Date'

    const milliseconds = timestamp < MILLISECONDS_MULTIPLIER ? timestamp * 1000 : timestamp
    
    return new Date(milliseconds).toLocaleString(undefined, dateFormatOptions)
  }
})()

const keyPatterns: Set<string> = new Set(['key', 'api_key', 'key_api', 'api', 'api-key'].map(s => s.toLowerCase()))

export function maskApiKeys(inputString: string): string {
  try {
    const data: { [key: string]: any } = JSON.parse(inputString)
    for (const [key, value] of Object.entries(data)) {
      if (keyPatterns.has(key.toLowerCase())) {
        data[key] = '*'.repeat(String(value).length)
      }
    }
    return JSON.stringify(data)
  } catch (error) {
    return inputString.replace(/"(key|api_key|key_api|api|api-key)":\s*"([^"]*)"/gi, 
      (_, key, value) => `"${key}":"${'*'.repeat(value.length)}"`)
  }
}
