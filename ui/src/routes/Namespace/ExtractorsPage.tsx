import { Box } from '@mui/material'
import { IExtractor } from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import ExtractorsTable from '../../components/tables/ExtractorsTable'

const ExtractorsPage = () => {
  const {
    extractors
  } = useLoaderData() as {
    extractors:IExtractor[]
  }
  return <Box><ExtractorsTable extractors={extractors}/></Box>
}

export default ExtractorsPage
