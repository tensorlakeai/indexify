import React, { useState, useEffect } from 'react';
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
} from '@mui/material';

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
  invocationId: string;
  namespace: string;
  computeGraph: string;
}

const InvocationTasksTable: React.FC<InvocationTasksTableProps> = ({ invocationId, namespace, computeGraph }) => {
  const [tasks, setTasks] = useState<Task[]>([]);

  useEffect(() => {
    const fetchTasks = async () => {
      try {
        const url = `http://localhost:8900/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/tasks`;
        const response = await axios.get<{ tasks: Task[] }>(url, {
          headers: {
            'accept': 'application/json'
          }
        });
        setTasks(response.data.tasks);
      } catch (error) {
        console.error('Error fetching tasks:', error);
      }
    };

    fetchTasks();
  }, [invocationId, namespace, computeGraph]);

  if (!tasks || tasks.length === 0) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Tasks Found
        </Alert>
      </Box>
    );
  }

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

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Tasks for Invocation</Typography>
      <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Compute Function</TableCell>
              <TableCell>Input Key</TableCell>
              <TableCell>Outcome</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tasks.map((task) => (
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
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default InvocationTasksTable;
