import { useEffect, useMemo, useState } from 'react';
import { useLoaderData } from 'react-router-dom';
import {
  Box,
  Typography,
  Stack,
  Breadcrumbs,
  Chip,
  Tooltip,
} from '@mui/material';
import {
  ExtractionGraph,
  IContentMetadata,
  IExtractionPolicy,
  IndexifyClient,
} from 'getindexify';
import TasksTable from '../../components/TasksTable';
import { Link } from 'react-router-dom';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import TasksContentDrawer from '../../components/TasksContentDrawer';

const ExtractionPolicyPage = () => {
  const { policy, namespace, extractionGraph, client } =
    useLoaderData() as {
      policy: IExtractionPolicy;
      namespace: string;
      client: IndexifyClient;
      extractionGraph: ExtractionGraph;
    };

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
    pending: 0,
    success: 0,
    failure: 0
  });

  const fetchTaskCounts = useMemo(() => {
    return async () => {
      try {
        const results =  await client.getExtractionGraphAnalytics({ namespace, extractionGraph: extractionGraph.name });
        const { failure, pending, success } = results.task_analytics[policy.name]

        return { pending, success, failure };
      } catch (error) {
        console.error("Error fetching task counts:", error);
        return { pending: 0, success: 0, failure: 0 };
      }
    };
  }, [client, extractionGraph.name, policy.name, namespace]);

  useEffect(() => {
    const loadTaskCounts = async () => {
      const taskCounts = await fetchTaskCounts();
      setLocalTaskCounts(taskCounts)
  };

  loadTaskCounts();
}, [fetchTaskCounts]);

  const [selectedContent, setSelectedContent] = useState<IContentMetadata | undefined>(undefined);
  const [drawerOpen, setDrawerOpen] = useState(false);

  const handleContentClick = async (contentId: string) => {
    try {
      const contentMetadata = await client.getContentMetadata(contentId);
      setSelectedContent(contentMetadata);
      setDrawerOpen(true);
    } catch (error) {
      console.error('Error fetching content metadata:', error);
    }
  };

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
        <Link color="inherit" to={`/${namespace}/extraction-graphs/${extractionGraph.name}`}>
          <Typography color="text.primary">{extractionGraph.name}</Typography>
        </Link>
        <Typography color="text.primary">Extraction Policies</Typography>
        <Typography color="text.primary">{policy.name}</Typography>
      </Breadcrumbs>
      <Box display="flex" flexDirection="row" alignItems="center" justifyContent="space-between">
        <Box display={'flex'} alignItems={'center'}>
        <Typography variant="h2" component="h1">
          Extraction Policy Tasks - {policy.name}
        </Typography>
      </Box>
      <Box>
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
                label={localTaskCounts.pending}
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
      </Box>
      </Box>
      <TasksTable
        namespace={namespace}
        loadData={taskLoader}
        extractionPolicies={extractionGraph.extraction_policies}
        onContentClick={handleContentClick}
        client={client}
        hideExtractionPolicy
      />
      <TasksContentDrawer 
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        content={selectedContent}
        client={client}
      />
    </Stack>
  )
}

export default ExtractionPolicyPage;
