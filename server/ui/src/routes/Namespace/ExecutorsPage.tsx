import { Box } from '@mui/material'
import { useLoaderData } from 'react-router-dom'
import ExecutorsCard from '../../components/cards/ExecutorsCard'
import { ExecutorMetadata } from 'getindexify'

const ExecutorsPage = () => {
  const {
    executors
  } = useLoaderData() as {
    executors: ExecutorMetadata[]
  }
  return <Box><ExecutorsCard executors={executors}/></Box>
}

export default ExecutorsPage;