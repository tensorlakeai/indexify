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

const InvocationTasksTable: React.FC<InvocationTasksTableProps> = ({ indexifyServiceURL, invocationId, namespace, computeGraph }) => {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [searchTerms, setSearchTerms] = useState<Record<string, string>>({});
  const [filteredTasks, setFilteredTasks] = useState<Record<string, Task[]>>({});
  const [expandedPanels, setExpandedPanels] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const fetchTasks = async () => {
      try {
        const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/tasks`;
        const response = await axios.get<{ tasks: Task[] }>(url, {
          headers: {
            'accept': 'application/json'
          }
        });
        setTasks(response.data.tasks);
      } catch (error) {
        console.error('Error fetching tasks:', error);
        toast.error('Failed to fetch tasks. Please try again later.');
      }
    };

    fetchTasks();
  }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

  useEffect(() => {
    const grouped = tasks.reduce((acc, task) => {
      if (!acc[task.compute_fn]) {
        acc[task.compute_fn] = [];
      }
      acc[task.compute_fn].push(task);
      return acc;
    }, {} as Record<string, Task[]>);

    setFilteredTasks(grouped);

    const initialExpandedState = Object.keys(grouped).reduce((acc, key) => {
      acc[key] = true;
      return acc;
    }, {} as Record<string, boolean>);
    setExpandedPanels(initialExpandedState);
  }, [tasks]);

  useEffect(() => {
    const filtered = Object.keys(searchTerms).reduce((acc, computeFn) => {
      const term = searchTerms[computeFn].toLowerCase();
      acc[computeFn] = tasks.filter(task => 
        task.compute_fn === computeFn && 
        task.id.toLowerCase().includes(term)
      );
      return acc;
    }, {} as Record<string, Task[]>);

    tasks.forEach(task => {
      if (!searchTerms[task.compute_fn]) {
        if (!filtered[task.compute_fn]) {
          filtered[task.compute_fn] = [];
        }
        if (!filtered[task.compute_fn].find(t => t.id === task.id)) {
          filtered[task.compute_fn].push(task);
        }
      }
    });

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
      const response = await axios.get(url, {
        responseType: 'text',
        headers: {
          'accept': 'text/plain'
        }
      });

      const logContent = response.data;

      if (!logContent || logContent.trim() === '') {
        toast.info(`No ${logType} logs found for task ${task.id}.`);
        return;
      }

      const newWindow = window.open('', '_blank');
      if (newWindow) {
        newWindow.document.write(`
          <html>
            <head>
              <title>Task ${task.id} - ${logType} Log</title>
              <style>
                body { font-family: monospace; white-space: pre-wrap; word-wrap: break-word; }
              </style>
            </head>
            <body>${logContent}</body>
          </html>
        `);
        newWindow.document.close();
      } else {
        toast.error('Unable to open new window. Please check your browser settings.');
      }
    } catch (error) {
      if (axios.isAxiosError(error) && error.response) {
        if (error.response.status === 404) {
          toast.info(`No ${logType} logs found for task ${task.id}.`);
        } else {
          toast.error(`Failed to fetch ${logType} logs for task ${task.id}. Please try again later.`);
        }
      } else {
        toast.error(`An unexpected error occurred while fetching ${logType} logs for task ${task.id}.`);
      }
      console.error(`Error fetching ${logType} logs:`, error);
    }
  };

  const getChipStyles = (outcome: string) => {
    const baseStyle = {
      borderRadius: '16px',
      fontWeight: 'bold',
    };

    switch (outcome.toLowerCase()) {
      case 'success':
        return {
          ...baseStyle,
          color: '#008000b8',
          backgroundColor: 'rgb(0 200 0 / 19%)',
        };
      case 'failed':
        return {
          ...baseStyle,
          color: 'red',
          backgroundColor: 'rgba(255, 0, 0, 0.1)',
        };
      default:
        return {
          ...baseStyle,
          color: 'blue',
          backgroundColor: 'rgba(0, 0, 255, 0.1)',
        };
    }
  };

  if (!tasks || tasks.length === 0) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Tasks Found
        </Alert>
        <ToastContainer position="top-right" />
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Tasks for Invocation</Typography>
      {Object.entries(filteredTasks).map(([computeFn, tasks], index) => (
        <Accordion 
          key={index} 
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
};

export default InvocationTasksTable;
