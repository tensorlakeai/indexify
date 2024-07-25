import { ExtractionGraph, Extractor, IExtractedMetadata, IndexifyClient } from 'getindexify'
import { IHash } from '../types'

export const stringToColor = (str: string) => {
  let hash = 0
  str.split('').forEach((char) => {
    hash = char.charCodeAt(0) + ((hash << 5) - hash)
  })
  let color = '#'
  for (let i = 0; i < 3; i++) {
    const value = (hash >> (i * 8)) & 0xff
    color += value.toString(16).padStart(2, '0')
  }
  return color
}

export const groupMetadataByExtractor = (
  metadataArray: IExtractedMetadata[]
): Record<string, IExtractedMetadata[]> => {
  return metadataArray.reduce((accumulator, currentItem) => {
    // Use the extractor_name as the key
    const key = currentItem.extractor_name

    // If the key doesn't exist yet, initialize it
    if (!accumulator[key]) {
      accumulator[key] = []
    }

    // Add the current item to the appropriate group
    accumulator[key].push(currentItem)

    return accumulator
  }, {} as Record<string, IExtractedMetadata[]>)
}

export const getIndexifyServiceURL = (): string => {
  if (process.env.NODE_ENV === 'development') {
    return 'http://localhost:8900'
  }
  return window.location.origin
}

export const formatBytes = (bytes: number, decimals: number = 2): string => {
  if (!+bytes) return '0 Bytes'

  const k = 1000
  const dm = decimals < 0 ? 0 : decimals
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}

export const getExtractionPolicyTaskCounts = async (
  extractionGraph: string,
  extractionPolicyName: string,
  client: IndexifyClient
): Promise<any> => {
  const tasks = client.getTasks(
      extractionGraph,
      extractionPolicyName,
    );
    return tasks;
}

type KeyValueObject = { [key: string]: string };

export const splitLabels = (data: KeyValueObject): string[] => {
  return Object.entries(data).map(([key, value]) => `${key}: ${value}`);
}

export interface Row {
    id: number;
    name: string;
    extractor: string;
    inputTypes: string[];
    inputParameters: string;
    pending: number;
    failed: number;
    completed: number;
}

export const mapExtractionPoliciesToRows = (
  extractionGraph: ExtractionGraph,
  extractors: Extractor[],
  graphName: string,
  tasks: IHash
): Row[] => {
  const extractorMap = new Map(extractors.map(e => [e.name, e]));
  
  let targetGraph: ExtractionGraph | undefined;
  
  if (Array.isArray(extractionGraph)) {
    targetGraph = extractionGraph.find(graph => graph.name === graphName);
  } else if (extractionGraph.name === graphName) {
    targetGraph = extractionGraph;
  }
  
  if (!targetGraph) {
    console.error(`No graph found with name: ${graphName}`);
    return [];
  }
  
  const rows: Row[] = targetGraph.extraction_policies.map((policy, index) => {
    const extractor = extractorMap.get(policy.extractor);
    const policyTasks = tasks[policy.name] || [];
    const pendingTaskCount = policyTasks.totalTasks?.unknown ?? 0;
    const failedTaskCount = policyTasks.totalTasks?.failure ?? 0;
    const completedTaskCount = policyTasks.totalTasks?.success ?? 0;
    
    const finalRows = {
      id: index + 1,
      name: policy.name,
      extractor: policy.extractor,
      inputTypes: extractor ? extractor.input_mime_types : ['Unknown'],
      inputParameters: policy.input_params ? JSON.stringify(policy.input_params) : 'None',
      pending: pendingTaskCount,
      failed: failedTaskCount,
      completed: completedTaskCount,
    };
    return finalRows;
  });

  return rows;
};

export async function getTasksForExtractionGraph(
  extractionGraph: ExtractionGraph,
  client: any,
): Promise<IHash> {
  const tasks_by_policies: IHash = {};

  for (const extractionPolicy of extractionGraph.extraction_policies) {
    tasks_by_policies[extractionPolicy.name] = await client.getTasks(
      extractionGraph.name,
      extractionPolicy.name
    );
  }
  return tasks_by_policies;
}

export const formatTimestamp = (value: string | number | null | undefined): string => {
  if (value == null) return 'N/A';
  
  let timestamp: number;
  
  if (typeof value === 'string') {
    timestamp = parseInt(value, 10);
  } else if (typeof value === 'number') {
    timestamp = value;
  } else {
    return 'Invalid Date';
  }
  
  if (isNaN(timestamp)) return 'Invalid Date';

  const milliseconds = timestamp < 1e12 ? timestamp * 1000 : timestamp;
  
  const date = new Date(milliseconds);
  
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });
};

const keyPatterns: string[] = ['key', 'api_key', 'key_api', 'API', 'KEY', 'API_KEY', 'API-KEY'];

export function maskApiKeys(inputString: string): string {
  try {
    const data: { [key: string]: any } = JSON.parse(inputString);
    const maskValue = (value: any): string => '*'.repeat(String(value).length);
    for (const [key, value] of Object.entries(data)) {
      if (keyPatterns.some(pattern => key.toLowerCase().includes(pattern.toLowerCase()))) {
        data[key] = maskValue(value);
      }
    }

    return JSON.stringify(data);
  } catch (error) {
    let result = inputString;
    for (const pattern of keyPatterns) {
      const regex = new RegExp(`("${pattern}":\\s*")([^"]*)`, 'gi');
      result = result.replace(regex, (_, g1, g2) => `${g1}${'*'.repeat(g2.length)}`);
    }
    return result;
  }
}
