import React from 'react';
import { TableContainer, Paper, Table, TableHead, TableRow, TableCell, TableBody, Box, Chip } from '@mui/material';
import { Row } from '../../utils/helpers';

interface ExtractorTableProps {
  rows: Row[];
  graphName?: string; // Added this prop for the table title
}

const ExtractorGraphTable: React.FC<ExtractorTableProps> = ({ rows, graphName }) => {
  return (
    <TableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: 'none'}}>
      <Table sx={{ minWidth: 650 }} aria-label="extractor table">
        <TableHead sx={{ pt: 2}}>
          <TableRow sx={{ mt: 2}}>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Name</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Extractor</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Input Types</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Input Parameters</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Pending</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Failed</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Completed</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell sx={{ pt: 2}}>
                <Box sx={{ display: 'flex', alignItems: 'left', flexDirection: 'column' }}>
                  <Box sx={{ mr: 1, display: 'flex' }}>
                    {Array(row.id).fill(0).map((_, i) => (
                      <Box key={i} sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#6FA8EA', mr: 0.5 }} />
                    ))}
                  </Box>
                  {row.name}
                </Box>
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.extractor}</TableCell>
              <TableCell sx={{ pt: 2}}>
                {row.inputTypes.map((type, index) => (
                  <Chip key={index} label={type} size="small" sx={{ mr: 0.5 }} />
                ))}
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.inputParameters}</TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.pending} sx={{ backgroundColor: '#E5EFFB' }} /></TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.failed} sx={{ backgroundColor: '#FBE5E5' }} /></TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.completed} sx={{ backgroundColor: '#E5FBE6' }} /></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default ExtractorGraphTable;