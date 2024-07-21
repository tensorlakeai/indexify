import React from 'react';
import { TableContainer as MuiTableContainer, Paper, Table as MuiTable, TableHead as MuiTableHead, TableRow as MuiTableRow, TableCell as MuiTableCell, TableBody as MuiTableBody, Box, Chip, Stack, Tooltip, Link } from '@mui/material';
import { Row } from '../../utils/helpers';
import CopyText from '../../components/CopyText';

interface ExtractorTableProps {
  rows: Row[];
  namespace: string;
  extractionPolicyName: string;
  graphName?: string;
}

interface StatusChipProps {
  label: string;
  value: number;
  color: string;
}

interface StatusChipsProps {
  pending: number;
  failed: number;
  completed: number;
  href: string;
}

const StatusChip: React.FC<StatusChipProps> = ({ label, value, color }) => (
  <Tooltip title={`${label}: ${value}`} arrow>
    <Chip 
      label={value} 
      sx={{ 
        backgroundColor: color,
        '&:hover': { backgroundColor: color },
      }} 
    />
  </Tooltip>
);

const StatusChips: React.FC<StatusChipsProps> = ({ pending, failed, completed, href }) => (
  <Link href={href} underline="none" onClick={(e: React.MouseEvent) => e.stopPropagation()}>
    <Stack direction="row" spacing={1} sx={{ cursor: 'pointer' }}>
      <StatusChip label="Pending" value={pending} color="#E5EFFB" />
      <StatusChip label="Failed" value={failed} color="#FBE5E5" />
      <StatusChip label="Completed" value={completed} color="#E5FBE6" />
    </Stack>
  </Link>
);

const ExtractorGraphTable: React.FC<ExtractorTableProps> = ({ rows, namespace, extractionPolicyName, graphName }) => {
  return (
    <MuiTableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
      <MuiTable sx={{ minWidth: 650 }} aria-label="extractor table">
        <MuiTableHead sx={{ pt: 2}}>
          <MuiTableRow sx={{ mt: 2}}>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Name</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Extractor</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Input Types</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Input Parameters</MuiTableCell>
            <MuiTableCell sx={{ fontSize: 14, pt: 1}}>Status</MuiTableCell>
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
              <MuiTableCell sx={{ pt: 2}}>
                <StatusChips
                  pending={row.pending}
                  failed={row.failed}
                  completed={row.completed}
                  href={`#`}
                />
              </MuiTableCell>
            </MuiTableRow>
          ))}
        </MuiTableBody>
      </MuiTable>
    </MuiTableContainer>
  );
};

export default ExtractorGraphTable;