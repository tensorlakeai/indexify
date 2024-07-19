import React from 'react';
import { TableContainer as MuiTableContainer, Paper, Table as MuiTable, TableHead as MuiTableHead, TableRow as MuiTableRow, TableCell as MuiTableCell, TableBody as MuiTableBody, Box, Chip } from '@mui/material';
import { Row } from '../../utils/helpers';
import CopyText from '../../components/CopyText';

interface ExtractorTableProps {
  rows: Row[];
  graphName?: string;
}

const ExtractorGraphTable: React.FC<ExtractorTableProps> = ({ rows, graphName }) => {
  return (
    <MuiTableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
      <MuiTable sx={{ minWidth: 650 }} aria-label="extractor table">
        <MuiTableHead sx={{ pt: 2}}>
          <MuiTableRow sx={{ mt: 2}}>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Name</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Extractor</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Input Types</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Input Parameters</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Pending</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Failed</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Completed</MuiTableCell>
          </MuiTableRow>
        </MuiTableHead>
        <MuiTableBody>
          {rows.map((row) => (
            <MuiTableRow key={row.id}>
              <MuiTableCell sx={{ pt: 2}}>
                <Box sx={{ display: 'flex', alignItems: 'left', flexDirection: 'column' }}>
                  <Box sx={{ mr: 1, display: 'flex' }}>
                    {Array(row.id).fill(0).map((_, i) => (
                      <Box key={i} sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#6FA8EA', mr: 0.5 }} />
                    ))}
                  </Box>
                  <Box display={'flex'} flexDirection={'row'} alignItems={'center'}>
                  {row.name}
                  <CopyText text={row.name} />
                  </Box>
                </Box>
              </MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}>{row.extractor}</MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}>
                {row.inputTypes.map((type, index) => (
                  <Chip key={index} label={type} size="small" sx={{ mr: 0.5 }} />
                ))}
              </MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}>{row.inputParameters}</MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}><Chip label={row.pending} sx={{ backgroundColor: '#E5EFFB' }} /></MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}><Chip label={row.failed} sx={{ backgroundColor: '#FBE5E5' }} /></MuiTableCell>
              <MuiTableCell sx={{ pt: 2}}><Chip label={row.completed} sx={{ backgroundColor: '#E5FBE6' }} /></MuiTableCell>
            </MuiTableRow>
          ))}
        </MuiTableBody>
      </MuiTable>
    </MuiTableContainer>
  );
};

export default ExtractorGraphTable;
