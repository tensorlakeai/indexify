import { Box } from '@mui/material'
import { useLoaderData } from 'react-router-dom'
import NamespacesCard from '../../components/cards/NamespacesCard'
import { Namespace } from '../../types'

const NamespacesPage = () => {
  const {
    namespaces
  } = useLoaderData() as {
    namespaces: Namespace[]
  }
  return <Box><NamespacesCard namespaces={namespaces}/></Box>
}

export default NamespacesPage;
