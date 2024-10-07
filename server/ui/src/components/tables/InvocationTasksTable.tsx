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
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

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
      }
    };

    fetchTasks();
  }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

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
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default InvocationTasksTable;
