import { Box } from '@mui/material'
import {
  ExtractionGraph,
  IIndex
} from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import IndexTable from '../../components/tables/IndexTable'

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

export default IndexesPage;
