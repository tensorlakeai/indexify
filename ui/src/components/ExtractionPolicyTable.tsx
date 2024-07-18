import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Box,
  Typography,
} from '@mui/material';
import { Link } from 'react-router-dom';
import CopyText from './CopyText';

interface ExtractionPolicyProps {
  id: string;
  name: string;
  extractor: string;
  inputTypes: string[];
  inputParameters: string | null;
  pending: number;
  failed: number;
  completed: number;
  depth: number;
}

const ExtractionPolicyTable: React.FC = () => {
  const policies: ExtractionPolicyProps[] = [
    {
      id: '1',
      name: 'moondreamextractor',
      extractor: 'tensorlake/moondream',
      inputTypes: ['image/bmp', 'image/gif'],
      inputParameters: null,
      pending: 20,
      failed: 12,
      completed: 24,
      depth: 0,
    },
  ];

  const renderName = (name: string, depth: number) => (
    <Box sx={{ display: 'flex', alignItems: 'center' }}>
      {[...Array(depth)].map((_, i) => (
        <span key={i} style={{ marginRight: 8 }}>•</span>
      ))}
      <Link to={`/policies/${name}`}>{name}</Link>
    </Box>
  );

  return (
    <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <Typography variant="h6" sx={{ p: 2 }}>
        moondreamcaptionkb
      </Typography>
      <CopyText text={"moondreamcaptionkb"} />
        </div>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell sx={{ fontSize: 14}}>Name</TableCell>
            <TableCell sx={{ fontSize: 14}}>Extractor</TableCell>
            <TableCell sx={{ fontSize: 14}}>Input Types</TableCell>
            <TableCell sx={{ fontSize: 14}}>Input Parameters</TableCell>
            <TableCell sx={{ fontSize: 14}}>Pending</TableCell>
            <TableCell sx={{ fontSize: 14}}>Failed</TableCell>
            <TableCell sx={{ fontSize: 14}}>Completed</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {policies.map((policy) => (
            <TableRow key={policy.id}>
              <TableCell>{renderName(policy.name, policy.depth)}</TableCell>
              <TableCell>{policy.extractor}</TableCell>
              <TableCell>
                {policy.inputTypes.map((type) => (
                  <Chip key={type} label={type} sx={{ mr: 0.5, backgroundColor: '#E5EFFB' }} />
                ))}
              </TableCell>
              <TableCell>
                <Chip label={policy.inputParameters || 'None'} />
              </TableCell>
              <TableCell>
                <Chip label={policy.pending} sx={{ backgroundColor: '#E5EFFB' }} />
              </TableCell>
              <TableCell>
                <Chip label={policy.failed} sx={{ backgroundColor: '#FBE5E5' }} />
              </TableCell>
              <TableCell>
                <Chip label={policy.completed} sx={{ backgroundColor: '#E5FBE6' }} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default ExtractionPolicyTable;
