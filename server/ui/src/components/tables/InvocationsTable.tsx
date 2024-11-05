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
import axios from 'axios';
import { Link } from 'react-router-dom';
import CopyText from '../CopyText';
import { formatBytes, formatTimestamp, getIndexifyServiceURL } from '../../utils/helpers';
import { DataObject } from '../../types';

interface InvocationsTableProps {
  invocationsList: DataObject[];
  computeGraph: string;
  namespace: string;
  onDelete: (updatedList: DataObject[]) => void;
}

export function InvocationsTable({ invocationsList, computeGraph, onDelete, namespace }: InvocationsTableProps) {
  const handleDelete = async (invocationId: string) => {
    try {
      await axios.delete(
        `${getIndexifyServiceURL()}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}`,
        { headers: { accept: '*/*' } }
      );
      
      onDelete(invocationsList.filter(invocation => invocation.id !== invocationId));
    } catch (error) {
      console.error(`Error deleting invocation ${invocationId}:`, error);
    }
  };

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Invocations</Typography>
      <TableContainer component={Paper} sx={{ boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Created At</TableCell>
              <TableCell>Payload Size</TableCell>
              <TableCell>Action</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {invocationsList.map(({ id, created_at, payload_size }) => (
              <TableRow key={id}>
                <TableCell>
                  <Box display="flex" flexDirection="row" alignItems="center">
                    <Link to={`/${namespace}/compute-graphs/${computeGraph}/invocations/${id}`}>
                      {id}
                    </Link>
                    <CopyText text={id} />
                  </Box>
                </TableCell>
                <TableCell>{formatTimestamp(created_at)}</TableCell>
                <TableCell>{formatBytes(payload_size)}</TableCell>
                <TableCell>
                  <IconButton onClick={() => handleDelete(id)} color="error" size="small">
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
}

export default InvocationsTable;
