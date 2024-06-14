import { Box } from '@mui/material'
import { IExtractor, IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, useLoaderData } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'
import ExtractorsTable from '../../components/tables/ExtractorsTable'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })

  const extractors = await client.extractors()
  return { extractors }
}

const ExtractorsPage = () => {
  const {
    extractors
  } = useLoaderData() as {
    extractors:IExtractor[]
  }
  return <Box><ExtractorsTable extractors={extractors}/></Box>
}

export default ExtractorsPage
