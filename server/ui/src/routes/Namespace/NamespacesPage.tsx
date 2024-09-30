import { Box } from '@mui/material'
import { Namespace } from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import NamespacesCard from '../../components/cards/NamespacesCard'

const NamespacesPage = () => {
  const {
    namespaces
  } = useLoaderData() as {
    namespaces: Namespace[]
  }
  return <Box><NamespacesCard namespaces={namespaces}/></Box>
}

export default NamespacesPage;