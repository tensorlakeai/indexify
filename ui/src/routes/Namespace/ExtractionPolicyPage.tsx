import { useLoaderData } from 'react-router-dom'
import {
  Box,
  Typography,
  Stack,
  Breadcrumbs,
  Tooltip,
  Chip,
} from '@mui/material'
import {
  ExtractionGraph,
  IExtractionPolicy,
  IndexifyClient,
  ITask,
} from 'getindexify'
import TasksTable from '../../components/TasksTable'
import { Link } from 'react-router-dom'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { TaskCounts } from '../../types'

const ExtractionPolicyPage = () => {
  const { policy, namespace, extractionGraph, taskCounts, client } =
    useLoaderData() as {
      policy: IExtractionPolicy
      namespace: string
      client: IndexifyClient
      extractionGraph: ExtractionGraph
      taskCounts?: TaskCounts
    }

  const taskLoader = async (
    pageSize: number,
    startId?: string
  ): Promise<ITask[]> => {
    const { tasks } = await client.getTasks({
      returnTotal: false,
      extractionPolicyId: policy.id,
      limit: pageSize + 1,
      startId,
    })
    return tasks
  }

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/extraction-graphs`}>
          <Typography color="text.primary">Extraction Graphs</Typography>
        </Link>
        <Typography color="text.primary">{policy.graph_name}</Typography>
        <Typography color="text.primary">{policy.name}</Typography>
      </Breadcrumbs>
      <Box display={'flex'} alignItems={'center'}>
        <Typography variant="h2" component="h1">
          Extraction Policy - {policy.name}
        </Typography>
      </Box>
      <Box>
        {taskCounts && (
          <Stack
            direction="row"
            spacing={1}
            display={'flex'}
            alignItems={'center'}
          >
            <Typography variant="body1">Tasks</Typography>
            <Tooltip title="In Progress">
              <Chip
                sx={{ backgroundColor: '#E5EFFB' }}
                label={taskCounts.totalUnknown}
              />
            </Tooltip>
            <Tooltip title="Failed">
              <Chip
                sx={{ backgroundColor: '#FBE5E5' }}
                label={taskCounts.totalFailed}
              />
            </Tooltip>
            <Tooltip title="Success">
              <Chip
                sx={{ backgroundColor: '#E5FBE6' }}
                label={taskCounts.totalSuccess}
              />
            </Tooltip>
          </Stack>
        )}
      </Box>
      <TasksTable
        namespace={namespace}
        loadData={taskLoader}
        extractionPolicies={extractionGraph.extraction_policies}
        hideExtractionPolicy
      />
    </Stack>
  )
}

export default ExtractionPolicyPage
