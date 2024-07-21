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
} from 'getindexify'
import TasksTable from '../../components/TasksTable'
import { Link } from 'react-router-dom'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { TaskCounts } from '../../types'
import { useEffect, useMemo, useState } from 'react'

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
): Promise<any> => {
  try {
    const tasks = await client.getTasks(
      extractionGraph.name,
      policy.name,
      {
        namespace: namespace,
        extractionGraph: extractionGraph.name,
        extractionPolicy: policy.name,
        limit: pageSize + 1,
        startId: startId,
        returnTotal: true
      }
    );

    let hasNextPage = false;
    if (tasks.tasks.length > pageSize) {
      hasNextPage = true;
      tasks.tasks.pop();
    }

    return {
      tasks: tasks.tasks,
      total: tasks.totalTasks,
      hasNextPage
    };
  } catch (error) {
    console.error("Error loading tasks:", error);
    return {
      tasks: [],
      total: 0,
      hasNextPage: false
    };
  }
};
  const [localTaskCounts, setLocalTaskCounts] = useState({
    unknown: 0,
    success: 0,
    failure: 0
  });

  const fetchTaskCounts = useMemo(() => {
    return async () => {
      try {
        const unknown = await client.getTasks(
          extractionGraph.name,
          policy.name,
          {
            namespace: namespace,
            extractionGraph: extractionGraph.name,
            extractionPolicy: policy.name,
            outcome: 'Unknown',
            returnTotal: true
          }
        );

        const success = await client.getTasks(
          extractionGraph.name,
          policy.name,
          {
            namespace: namespace,
            extractionGraph: extractionGraph.name,
            extractionPolicy: policy.name,
            outcome: 'Success',
            returnTotal: true
          }
        );

        const failure = await client.getTasks(
          extractionGraph.name,
          policy.name,
          {
            namespace: namespace,
            extractionGraph: extractionGraph.name,
            extractionPolicy: policy.name,
            outcome: 'Failed',
            returnTotal: true
          }
        );

        return {
          unknown: unknown.totalTasks || 0,
          success: success.totalTasks || 0,
          failure: failure.totalTasks || 0
        };
      } catch (error) {
        console.error("Error fetching task counts:", error);
        return { unknown: 0, success: 0, failure: 0 };
      }
    };
  }, [client, extractionGraph.name, policy.name, namespace]);

  useEffect(() => {
    fetchTaskCounts().then(setLocalTaskCounts);
  }, [fetchTaskCounts]);

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/indexes`}>
          <Typography color="text.primary">Indexes</Typography>
        </Link>
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
                label={localTaskCounts.unknown}
              />
            </Tooltip>
            <Tooltip title="Failed">
              <Chip
                sx={{ backgroundColor: '#FBE5E5' }}
                label={localTaskCounts.failure}
              />
            </Tooltip>
            <Tooltip title="Success">
              <Chip
                sx={{ backgroundColor: '#E5FBE6' }}
                label={localTaskCounts.success}
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
