import { Box } from '@mui/material'
import { ExtractionGraph, IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, useLoaderData } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'
import ExtractionGraphs from '../../components/ExtractionGraphs'
import { Extractor } from 'getindexify'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })
  const extractors = await client.extractors()
  return {
    namespace: client.namespace,
    extractionGraphs: client.extractionGraphs,
    extractors,
  }
}

const ExtractionGraphsPage = () => {
  const { extractors, extractionGraphs, namespace } = useLoaderData() as {
    extractionGraphs: ExtractionGraph[]
    namespace: string
    extractors: Extractor[]
  }
  return (
    <Box>
      <ExtractionGraphs
        extractors={extractors}
        namespace={namespace}
        extractionGraphs={extractionGraphs}
        tasks={[]}
      />
    </Box>
  )
}

export default ExtractionGraphsPage
