import { ExtractionGraph, Extractor, IExtractedMetadata, IndexifyClient } from 'getindexify'

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
  namespace: string,
  client: IndexifyClient
): Promise<any> => {
  const tasks = client.getTasks(
      extractionGraph,
      extractionPolicyName,
      namespace,
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
  tasks: any
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

  console.log('Tasks', tasks)
  
  const rows: Row[] = targetGraph.extraction_policies.map((policy, index) => {
    const extractor = extractorMap.get(policy.extractor);
    // const filterTasks = tasks.tasks.filter(task => task.extraction_policy_id === policy.id)
    // const pendingTaskCount = filterTasks.filter(task => task.outcome === 0).length
    // const failedTaskCount = filterTasks.filter(task => task.outcome === 1).length
    // const completedTaskCount = filterTasks.filter(task => task.outcome === 2).length
    const finalRows = {
      id: index + 1,
      name: policy.name,
      extractor: policy.extractor,
      inputTypes: extractor ? extractor.input_mime_types : ['Unknown'],
      inputParameters: policy.input_params ? JSON.stringify(policy.input_params) : 'None',
      pending: 0,
      failed: 0,
      completed: 0,
    };
    return finalRows;
  });

  return rows;
};
