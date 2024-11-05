import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  Box,
  Alert,
  Chip,
  TextField,
  InputAdornment,
  Button,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import SearchIcon from '@mui/icons-material/Search';
import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

interface Task {
  id: string;
  namespace: string;
  compute_fn: string;
  compute_graph: string;
  invocation_id: string;
  input_key: string;
  outcome: string;
}

interface InvocationTasksTableProps {
  indexifyServiceURL: string;
  invocationId: string;
  namespace: string;
  computeGraph: string;
}

const OUTCOME_STYLES = {
  success: {
    color: '#008000b8',
    backgroundColor: 'rgb(0 200 0 / 19%)',
  },
  failed: {
    color: 'red',
    backgroundColor: 'rgba(255, 0, 0, 0.1)',
  },
  default: {
    color: 'blue',
    backgroundColor: 'rgba(0, 0, 255, 0.1)',
  },
} as const;

export function InvocationTasksTable({ indexifyServiceURL, invocationId, namespace, computeGraph }: InvocationTasksTableProps) {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [searchTerms, setSearchTerms] = useState<Record<string, string>>({});
  const [filteredTasks, setFilteredTasks] = useState<Record<string, Task[]>>({});
  const [expandedPanels, setExpandedPanels] = useState<Record<string, boolean>>({});

  const fetchTasks = async () => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/tasks`;
      const response = await axios.get<{ tasks: Task[] }>(url, {
        headers: { accept: 'application/json' }
      });
      setTasks(response.data.tasks);
    } catch (error) {
      console.error('Error fetching tasks:', error);
      toast.error('Failed to fetch tasks. Please try again later.');
    }
  };

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { fetchTasks(); }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

  useEffect(() => {
    const grouped = tasks.reduce((acc, task) => ({
      ...acc,
      [task.compute_fn]: [...(acc[task.compute_fn] || []), task],
    }), {} as Record<string, Task[]>);

    setFilteredTasks(grouped);
    setExpandedPanels(Object.keys(grouped).reduce((acc, key) => ({ ...acc, [key]: true }), {}));
  }, [tasks]);

  useEffect(() => {
    const filtered = tasks.reduce((acc, task) => {
      const searchTerm = searchTerms[task.compute_fn]?.toLowerCase();
      if (!searchTerm || task.id.toLowerCase().includes(searchTerm)) {
        return {
          ...acc,
          [task.compute_fn]: [...(acc[task.compute_fn] || []), task],
        };
      }
      return acc;
    }, {} as Record<string, Task[]>);

    setFilteredTasks(filtered);
  }, [tasks, searchTerms]);

  const handleAccordionChange = (panel: string) => (event: React.SyntheticEvent, isExpanded: boolean) => {
    const target = event.target as HTMLElement;
    if (target.classList.contains('MuiAccordionSummary-expandIconWrapper') || 
        target.closest('.MuiAccordionSummary-expandIconWrapper')) {
      setExpandedPanels(prev => ({ ...prev, [panel]: isExpanded }));
    }
  };

  const viewLogs = async (task: Task, logType: 'stdout' | 'stderr') => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/fn/${task.compute_fn}/tasks/${task.id}/logs/${logType}`;
      const { data: logContent } = await axios.get(url, {
        responseType: 'text',
        headers: { accept: 'text/plain' }
      });

      if (!logContent?.trim()) {
        toast.info(`No ${logType} logs found for task ${task.id}.`);
        return;
      }

      const newWindow = window.open('', '_blank');
      if (!newWindow) {
        toast.error('Unable to open new window. Please check your browser settings.');
        return;
      }

      newWindow.document.write(`
        <html>
          <head>
            <title>Task ${task.id} - ${logType} Log</title>
            <style>body { font-family: monospace; white-space: pre-wrap; word-wrap: break-word; }</style>
          </head>
          <body>${logContent}</body>
        </html>
      `);
      newWindow.document.close();
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        toast.info(`No ${logType} logs found for task ${task.id}.`);
      } else {
        toast.error(`Failed to fetch ${logType} logs for task ${task.id}. Please try again later.`);
        console.error(`Error fetching ${logType} logs:`, error);
      }
    }
  };

  const getChipStyles = (outcome: string) => ({
    borderRadius: '16px',
    fontWeight: 'bold',
    ...(OUTCOME_STYLES[outcome.toLowerCase() as keyof typeof OUTCOME_STYLES] || OUTCOME_STYLES.default),
  });

  if (!tasks?.length) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">No Tasks Found</Alert>
        <ToastContainer position="top-right" />
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Tasks for Invocation</Typography>
      {Object.entries(filteredTasks).map(([computeFn, tasks], index) => (
        <Accordion 
          key={computeFn}
          expanded={expandedPanels[computeFn]}
          onChange={handleAccordionChange(computeFn)}
        >
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls={`panel${index}-content`}
            id={`panel${index}-header`}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', width: '100%', justifyContent: 'space-between' }}>
              <Typography>{computeFn} ({tasks.length} tasks)</Typography>
              <Box 
                sx={{ display: 'flex', alignItems: 'center' }}
                onClick={(e) => e.stopPropagation()}
              >
                <TextField
                  size="small"
                  placeholder="Search by ID"
                  value={searchTerms[computeFn] || ''}
                  onChange={(e) => setSearchTerms(prev => ({ ...prev, [computeFn]: e.target.value }))}
                  InputProps={{
                    endAdornment: (
                      <InputAdornment position="end">
                        <SearchIcon />
                      </InputAdornment>
                    ),
                  }}
                />
              </Box>
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}} elevation={0}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Outcome</TableCell>
                    <TableCell>Logs</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {tasks.map((task) => (
                    <TableRow key={task.id}>
                      <TableCell>{task.id}</TableCell>
                      <TableCell>
                        <Chip
                          label={task.outcome}
                          sx={getChipStyles(task.outcome)}
                        />
                      </TableCell>
                      <TableCell>
                        <Button 
                          onClick={() => viewLogs(task, 'stdout')}
                          sx={{ mr: 1 }}
                        >
                          View stdout
                        </Button>
                        <Button 
                          onClick={() => viewLogs(task, 'stderr')}
                        >
                          View stderr
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      ))}
      <ToastContainer position="top-right" />
    </Box>
  );
}

export default InvocationTasksTable;
