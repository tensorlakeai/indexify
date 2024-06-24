import { Box } from '@mui/material'
import { ISchema } from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import SchemasTable from '../../components/tables/SchemasTable'

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

export default SqlTablesPage;
