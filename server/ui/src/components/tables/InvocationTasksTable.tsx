import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import {
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
  const [searchTerm, setSearchTerm] = useState('');

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

  const filteredTasks = useMemo(() => {
    return tasks.filter(task => 
      task.id.toLowerCase().includes(searchTerm.toLowerCase()) ||
      task.compute_fn.toLowerCase().includes(searchTerm.toLowerCase()) ||
      task.input_key.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [tasks, searchTerm]);

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
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">Tasks for Invocation</Typography>
        <TextField
          size="small"
          placeholder="Search tasks"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon />
              </InputAdornment>
            ),
          }}
        />
      </Box>
      <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Compute Function</TableCell>
              <TableCell>Input Key</TableCell>
              <TableCell>Outcome</TableCell>
              <TableCell>Logs</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredTasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell>{task.id}</TableCell>
                <TableCell>{task.compute_fn}</TableCell>
                <TableCell sx={{ wordBreak: 'break-all' }}>{task.input_key}</TableCell>
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
      <ToastContainer position="top-right" />
    </Box>
  );
};

export default InvocationTasksTable;
