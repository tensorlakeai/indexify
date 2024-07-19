import React from 'react';
import {
  Table as MuiTable,
  TableBody as MuiTableBody,
  TableCell as MuiTableCell,
  TableContainer as MuiTableContainer,
  TableHead as MuiTableHead,
  TableRow as MuiTableRow,
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
    <MuiTableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <Typography variant="h6" sx={{ p: 2 }}>
        moondreamcaptionkb
      </Typography>
      <CopyText text={"moondreamcaptionkb"} />
        </div>
      <MuiTable>
        <MuiTableHead>
          <MuiTableRow>
            <MuiTableCell sx={{ fontSize: 14}}>Name</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Extractor</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Input Types</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Input Parameters</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Pending</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Failed</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14}}>Completed</MuiTableCell>
          </MuiTableRow>
        </MuiTableHead>
        <MuiTableBody>
          {policies.map((policy) => (
            <MuiTableRow key={policy.id}>
              <MuiTableCell>{renderName(policy.name, policy.depth)}</MuiTableCell>
              <MuiTableCell>{policy.extractor}</MuiTableCell>
              <MuiTableCell>
                {policy.inputTypes.map((type) => (
                  <Chip key={type} label={type} sx={{ mr: 0.5, backgroundColor: '#E5EFFB' }} />
                ))}
              </MuiTableCell>
              <MuiTableCell>
                <Chip label={policy.inputParameters || 'None'} />
              </MuiTableCell>
              <MuiTableCell>
                <Chip label={policy.pending} sx={{ backgroundColor: '#E5EFFB' }} />
              </MuiTableCell>
              <MuiTableCell>
                <Chip label={policy.failed} sx={{ backgroundColor: '#FBE5E5' }} />
              </MuiTableCell>
              <MuiTableCell>
                <Chip label={policy.completed} sx={{ backgroundColor: '#E5FBE6' }} />
              </MuiTableCell>
            </MuiTableRow>
          ))}
        </MuiTableBody>
      </MuiTable>
    </MuiTableContainer>
  );
};

export default ExtractionPolicyTable;
