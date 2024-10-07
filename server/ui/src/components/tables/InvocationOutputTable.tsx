import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  Box,
  Alert,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

interface Output {
  compute_fn: string;
  id: string;
}

interface InvocationOutputTableProps {
  indexifyServiceURL: string;
  invocationId: string;
  namespace: string;
  computeGraph: string;
}

const InvocationOutputTable: React.FC<InvocationOutputTableProps> = ({ indexifyServiceURL, invocationId, namespace, computeGraph }) => {
  const [outputs, setOutputs] = useState<Output[]>([]);

  useEffect(() => {
    const fetchOutputs = async () => {
      try {
        const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/outputs`;
        const response = await axios.get<{ outputs: Output[] }>(url, {
          headers: {
            'accept': 'application/json'
          }
        });
        setOutputs(response.data.outputs);
      } catch (error) {
        console.error('Error fetching outputs:', error);
      }
    };

    fetchOutputs();
  }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

  if (!outputs || outputs.length === 0) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Outputs Found
        </Alert>
      </Box>
    );
  }

  // Group outputs by compute_fn
  const groupedOutputs = outputs.reduce((acc, output) => {
    if (!acc[output.compute_fn]) {
      acc[output.compute_fn] = [];
    }
    acc[output.compute_fn].push(output);
    return acc;
  }, {} as Record<string, Output[]>);

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Outputs for Invocation</Typography>
      {Object.entries(groupedOutputs).map(([computeFn, outputs], index) => (
        <Accordion key={index} defaultExpanded>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls={`panel${index}-content`}
            id={`panel${index}-header`}
          >
            <Typography>Compute Function - {computeFn} ({outputs.length} outputs)</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",}} elevation={0}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Compute Function</TableCell>
                    <TableCell>ID</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {outputs.map((output, idx) => (
                    <TableRow key={idx}>
                      <TableCell>{output.compute_fn}</TableCell>
                      <TableCell>{output.id}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      ))}
    </Box>
  );
};

export default InvocationOutputTable;
