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
} from '@mui/material';

interface Output {
  compute_fn: string;
  id: string;
}

interface InvocationOutputTableProps {
  invocationId: string;
  namespace: string;
  computeGraph: string;
}

const InvocationOutputTable: React.FC<InvocationOutputTableProps> = ({ invocationId, namespace, computeGraph }) => {
  const [outputs, setOutputs] = useState<Output[]>([]);

  useEffect(() => {
    const fetchOutputs = async () => {
      try {
        const url = `http://localhost:8900/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/outputs`;
        const response = await axios.get<{ outputs: Output[] }>(url, {
          headers: {
            'accept': 'application/json'
          }
        });
        setOutputs(response.data.outputs);
      } catch (error) {
        console.error('Error fetching outputs:', error);
      }
    };

    fetchOutputs();
  }, [invocationId, namespace, computeGraph]);

  if (!outputs || outputs.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Outputs Found
          </Alert>
        </Box>
      );
    }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Outputs for Invocation</Typography>
      <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Compute Function</TableCell>
              <TableCell>ID</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {outputs.map((output, index) => (
              <TableRow key={index}>
                <TableCell>{output.compute_fn}</TableCell>
                <TableCell>{output.id}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default InvocationOutputTable;
