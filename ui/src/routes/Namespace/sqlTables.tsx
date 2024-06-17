import { Box } from '@mui/material'
import { ISchema, IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, useLoaderData } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'
import SchemasTable from '../../components/tables/SchemasTable'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })

  const schemas = await client.getSchemas()
  return { schemas }
}

const SqlTablesPage = () => {
  const { schemas } = useLoaderData() as {
    schemas: ISchema[]
  }
  return (
    <Box>
      <SchemasTable schemas={schemas} />
    </Box>
  )
}

export default SqlTablesPage
