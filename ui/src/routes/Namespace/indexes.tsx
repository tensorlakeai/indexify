import { Box } from '@mui/material'
import {
  ExtractionGraph,
  IIndex,
  IndexifyClient,
} from 'getindexify'
import { LoaderFunctionArgs, useLoaderData } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'
import IndexTable from '../../components/tables/IndexTable'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })

  const indexes = await client.indexes()
  return { indexes, namespace, extractionGraphs: client.extractionGraphs }
}

const IndexesPage = () => {
  const { indexes, namespace, extractionGraphs } = useLoaderData() as {
    indexes: IIndex[]
    namespace: string
    extractionGraphs: ExtractionGraph[]
  }
  return (
    <Box>
      <IndexTable
        indexes={indexes}
        namespace={namespace}
        extractionPolicies={extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
      />
    </Box>
  )
}

export default IndexesPage
