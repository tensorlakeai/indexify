import React from 'react';
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
  IconButton
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { DataObject } from 'getindexify';
import axios from 'axios';

interface InvocationsTableProps {
  invocationsList: DataObject[];
  computeGraph: string;
  namespace: string;
  onDelete: (updatedList: DataObject[]) => void;
}

const InvocationsTable: React.FC<InvocationsTableProps> = ({ invocationsList, computeGraph, onDelete, namespace }) => {
  const handleDelete = async (invocationId: string) => {
    try {
      const url = `http://localhost:8900/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}`;
      await axios.delete(url, {
        headers: {
          'accept': '*/*'
        }
      });
      
      const updatedList = invocationsList.filter(invocation => invocation.id !== invocationId);
      onDelete(updatedList);
      console.log(`Invocation ${invocationId} deleted successfully`);
    } catch (error) {
      console.error(`Error deleting invocation ${invocationId}:`, error);
    }
  };

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Invocations</Typography>
      <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Payload</TableCell>
              <TableCell>Payload Size</TableCell>
              <TableCell>Payload SHA-256</TableCell>
              <TableCell>Action</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {invocationsList.map((invocation) => (
              <TableRow key={invocation.id}>
                <TableCell>{invocation.id}</TableCell>
                <TableCell>{invocation.payload}</TableCell>
                <TableCell>{invocation.payload_size} bytes</TableCell>
                <TableCell sx={{ wordBreak: 'break-all' }}>{invocation.payload_sha_256}</TableCell>
                <TableCell>
                  <IconButton onClick={() => handleDelete(invocation.id)} color="error" size="small">
                    <DeleteIcon />
                  </IconButton>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default InvocationsTable;
