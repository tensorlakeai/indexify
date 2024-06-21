import { useLoaderData, LoaderFunctionArgs, redirect } from 'react-router-dom'
import { Box, Typography, Stack, Breadcrumbs } from '@mui/material'
import {
  ExtractionGraph,
  IExtractionPolicy,
  IndexifyClient,
  ITask,
} from 'getindexify'
import TasksTable from '../../components/TasksTable'
import { Link } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const policyname = params.policyname
  const graphname = params.graphname
  if (!namespace || !policyname) return redirect('/')

  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })
  const extractionGraph = client.extractionGraphs.find(
    (graph) => graph.name === graphname
  )
  const policy = client.extractionGraphs
    .map((graph) => graph.extraction_policies)
    .flat()
    .find(
      (policy) => policy.name === policyname && policy.graph_name === graphname
    )
  return { policy, namespace, extractionGraph, client }
}

const ExtractionPolicyPage = () => {
  const { policy, namespace, extractionGraph, client } = useLoaderData() as {
    policy: IExtractionPolicy
    namespace: string
    client: IndexifyClient
    extractionGraph: ExtractionGraph
  }

  const taskLoader = async (
    pageSize: number,
    startId?: string
  ): Promise<ITask[]> => {
    const tasks = await client.getTasks({
      extractionPolicyId: policy.id,
      limit: pageSize + 1,
      startId,
    })
    return tasks
  }

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
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
