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
  Button,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

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
        toast.error('Failed to fetch outputs. Please try again later.');
      }
    };

    fetchOutputs();
  }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

  const downloadLogs = async (fnName: string, logType: 'stdout' | 'stderr') => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/fn/${fnName}/logs/${logType}`;
      const response = await axios.get(url, {
        responseType: 'blob',
        headers: {
          'accept': 'application/octet-stream'
        }
      });

      if (response.data.size === 0) {
        toast.info(`No ${logType} logs available for ${fnName}.`);
        return;
      }

      const blob = new Blob([response.data], { type: 'application/octet-stream' });
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = `${fnName}_${logType}.log`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      toast.success(`${logType} logs for ${fnName} downloaded successfully.`);
    } catch (error) {
      console.error(`Error downloading ${logType} logs:`, error);
      toast.error(`Failed to download ${logType} logs for ${fnName}. Please try again later.`);
    }
  };

  if (!outputs || outputs.length === 0) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Outputs Found
        </Alert>
        <ToastContainer position="top-right" />
      </Box>
    );
  }

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
                    <TableCell>Logs</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {outputs.map((output, idx) => (
                    <TableRow key={idx}>
                      <TableCell>{output.compute_fn}</TableCell>
                      <TableCell>{output.id}</TableCell>
                      <TableCell>
                        <Button 
                          onClick={() => downloadLogs(output.compute_fn, 'stdout')}
                          sx={{ mr: 1 }}
                        >
                          Download stdout
                        </Button>
                        <Button 
                          onClick={() => downloadLogs(output.compute_fn, 'stderr')}
                        >
                          Download stderr
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      ))}
      <ToastContainer position="top-right" />
    </Box>
  );
};

export default InvocationOutputTable;
