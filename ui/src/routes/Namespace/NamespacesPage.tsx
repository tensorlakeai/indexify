import { Box } from '@mui/material'
import { Namespace } from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import NamespacesTable from '../../components/tables/NamespacesTable'

const NamespacesPage = () => {
  const {
    namespaces
  } = useLoaderData() as {
    namespaces: Namespace[]
  }
  return <Box><NamespacesTable namespaces={namespaces}/></Box>
}

export default NamespacesPage;