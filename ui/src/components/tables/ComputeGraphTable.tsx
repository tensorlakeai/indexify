import React from 'react';
import { TableContainer, Paper, Table, TableHead, TableRow, TableCell, TableBody, Box, Chip } from '@mui/material';
import CopyText from '../CopyText';
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
            <TableCell sx={{ fontSize: 14, pt: 1}}>Edges</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Function Name</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Description</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.sort((a, b) => a.name.localeCompare(b.name)).map((row) => (
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
              <TableCell sx={{ pt: 2}}>
                {row.dependencies.map((dep, index) => (
                  <Chip key={index} label={dep} size="small" sx={{ mr: 0.5 }} />
                ))}
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.fn_name}</TableCell>
              <TableCell sx={{ pt: 2}}>{row.description}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default ComputeGraphTable;
