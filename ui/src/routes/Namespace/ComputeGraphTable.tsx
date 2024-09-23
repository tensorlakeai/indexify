import React from 'react';
import { TableContainer, Paper, Table, TableHead, TableRow, TableCell, TableBody, Box, Chip, Stack, Tooltip, Link } from '@mui/material';
import CopyText from '../../components/CopyText';
import { ComputeGraph } from 'getindexify';

interface ComputeGraphTableProps {
  graphData: ComputeGraph;
  namespace: string;
}

interface RowData {
  name: string;
  type: 'compute_fn' | 'dynamic_router';
  fn_name: string;
  description: string;
  dependencies: string[];
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
  <Link href={href} underline="none" onClick={(e: React.MouseEvent<HTMLAnchorElement>) => e.stopPropagation()}>
    <Stack direction="row" spacing={1} sx={{ cursor: 'pointer' }}>
      <StatusChip label="Pending" value={pending} color="#E5EFFB" />
      <StatusChip label="Failed" value={failed} color="#FBE5E5" />
      <StatusChip label="Completed" value={completed} color="#E5FBE6" />
    </Stack>
  </Link>
);

const ComputeGraphTable: React.FC<ComputeGraphTableProps> = ({ graphData, namespace }) => {
  const rows: RowData[] = Object.entries(graphData.nodes).map(([nodeName, node]) => {
    if ('compute_fn' in node) {
      return {
        name: nodeName,
        type: 'compute_fn',
        fn_name: node.compute_fn.fn_name,
        description: node.compute_fn.description,
        dependencies: graphData.edges[nodeName] || [],
      };
    } else {
      return {
        name: nodeName,
        type: 'dynamic_router',
        fn_name: node.dynamic_router.source_fn,
        description: node.dynamic_router.description,
        dependencies: node.dynamic_router.target_fns,
      };
    }
  });

  return (
    <TableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
      <Table sx={{ minWidth: 650 }} aria-label="compute graph table">
        <TableHead sx={{ pt: 2}}>
          <TableRow sx={{ mt: 2}}>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Name</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Type</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Function Name</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Description</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Dependencies</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Status</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.name}>
              <TableCell sx={{ pt: 2}}>
                <Box sx={{ display: 'flex', alignItems: 'left', flexDirection: 'column' }}>
                  <Box sx={{ mr: 1, display: 'flex' }}>
                    <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#6FA8EA', mr: 0.5 }} />
                  </Box>
                  <Box display={'flex'} flexDirection={'row'} alignItems={'center'}>
                    {row.name}
                    <CopyText text={row.name} />
                  </Box>
                </Box>
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.type}</TableCell>
              <TableCell sx={{ pt: 2}}>{row.fn_name}</TableCell>
              <TableCell sx={{ pt: 2}}>{row.description}</TableCell>
              <TableCell sx={{ pt: 2}}>
                {row.dependencies.map((dep, index) => (
                  <Chip key={index} label={dep} size="small" sx={{ mr: 0.5 }} />
                ))}
              </TableCell>
              <TableCell sx={{ pt: 2}}>
                <StatusChips
                  pending={0}
                  failed={0}
                  completed={0}
                  href={`${namespace}/compute-graphs/${graphData.name}/nodes/${row.name}`}
                />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default ComputeGraphTable;
