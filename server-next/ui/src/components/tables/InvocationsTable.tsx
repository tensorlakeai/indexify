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
import { Link } from 'react-router-dom';
import CopyText from '../CopyText';

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
              <TableCell>Payload Size</TableCell>
              <TableCell>Action</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {invocationsList.map((invocation) => (
              <TableRow key={invocation.id}>
                <TableCell>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Link
                      to={`/${namespace}/compute-graphs/${computeGraph}/invocations/${invocation.id}`}
                    >
                      {invocation.id}
                    </Link>
                    <CopyText text={invocation.id}/>
                  </Box>
                </TableCell>
                <TableCell>{invocation.payload_size} bytes</TableCell>
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
