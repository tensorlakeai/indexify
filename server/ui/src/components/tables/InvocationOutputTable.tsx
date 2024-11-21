import React, { useState, useEffect, useCallback } from 'react';
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
  TextField,
  InputAdornment,
  Button,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import SearchIcon from '@mui/icons-material/Search';
import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { formatTimestamp } from '../../utils/helpers';

interface Output {
  compute_fn: string;
  id: string;
  created_at: string;
}

interface InvocationOutputTableProps {
  indexifyServiceURL: string;
  invocationId: string;
  namespace: string;
  computeGraph: string;
}

interface GroupedOutputs {
  [key: string]: Output[];
}

interface ExpandedPanels {
  [key: string]: boolean;
}

interface SearchTerms {
  [key: string]: string;
}

function InvocationOutputTable({ indexifyServiceURL, invocationId, namespace, computeGraph }: InvocationOutputTableProps) {
  const [outputs, setOutputs] = useState<Output[]>([]);
  const [searchTerms, setSearchTerms] = useState<SearchTerms>({});
  const [filteredOutputs, setFilteredOutputs] = useState<GroupedOutputs>({});
  const [expandedPanels, setExpandedPanels] = useState<ExpandedPanels>({});

  const fetchOutputs = useCallback(async () => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/outputs`;
      const response = await axios.get<{ outputs: Output[] }>(url, {
        headers: { accept: 'application/json' }
      });
      setOutputs(response.data.outputs);
    } catch (error) {
      console.error('Error fetching outputs:', error);
      toast.error('Failed to fetch outputs. Please try again later.');
    }
  }, [indexifyServiceURL, invocationId, namespace, computeGraph]);

  const fetchFunctionOutputPayload = useCallback(async (function_name: string, output_id: string) => {
    try {
      const url = `${indexifyServiceURL}/namespaces/${namespace}/compute_graphs/${computeGraph}/invocations/${invocationId}/fn/${function_name}/output/${output_id}`;
      const response = await axios.get(url);
      console.log("response", response.data,);
      const content_type = response.headers['content-type']
      const fileExtension = content_type === 'application/json' ? 'json' : 
          content_type === 'application/octet-stream' ? 'bin' : 'txt';
      const blob = new Blob([response.data], { type: content_type });
      const downloadLink = document.createElement('a');
      downloadLink.href = URL.createObjectURL(blob);
      downloadLink.download = `${function_name}_${output_id}_output.${fileExtension}`;
      document.body.appendChild(downloadLink);
      downloadLink.click();
      document.body.removeChild(downloadLink);
    } catch(error) {
      console.error(`Error fetching output for function: ${function_name} in compute graph: ${computeGraph} for output id: ${output_id}"`, error);
      toast.error(`Failed to fetch output for function: ${function_name} for output id: ${output_id}`)
    }
  }, [indexifyServiceURL, invocationId, namespace, computeGraph])

  const handleSearch = useCallback((computeFn: string, term: string) => {
    const filtered = outputs.filter(output => 
      output.compute_fn === computeFn && 
      output.id.toLowerCase().includes(term.toLowerCase())
    );
    setFilteredOutputs(prev => ({ ...prev, [computeFn]: filtered }));
  }, [outputs]);

  const handleAccordionChange = useCallback((panel: string) => 
    (event: React.SyntheticEvent, isExpanded: boolean) => {
      const target = event.target as HTMLElement;
      if (target.classList.contains('MuiAccordionSummary-expandIconWrapper') || 
          target.closest('.MuiAccordionSummary-expandIconWrapper')) {
        setExpandedPanels(prev => ({ ...prev, [panel]: isExpanded }));
      }
  }, []);

  useEffect(() => {
    fetchOutputs();
  }, [fetchOutputs]);

  useEffect(() => {
    const grouped = outputs.reduce((acc, output) => {
      if (!acc[output.compute_fn]) acc[output.compute_fn] = [];
      acc[output.compute_fn].push(output);
      return acc;
    }, {} as GroupedOutputs);

    setFilteredOutputs(grouped);
    setExpandedPanels(
      Object.keys(grouped).reduce((acc, key) => ({ ...acc, [key]: true }), {})
    );
  }, [outputs]);

  useEffect(() => {
    Object.entries(searchTerms).forEach(([computeFn, term]) => {
      handleSearch(computeFn, term);
    });
  }, [searchTerms, handleSearch]);

  if (!outputs.length) {
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">No Outputs Found</Alert>
        <ToastContainer position="top-right" />
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Typography variant="h6" gutterBottom>Outputs for Invocation</Typography>
      {Object.entries(filteredOutputs).map(([computeFn, outputs], index) => (
        <Accordion 
          key={index}
          expanded={expandedPanels[computeFn]}
          onChange={handleAccordionChange(computeFn)}
        >
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls={`panel${index}-content`}
            id={`panel${index}-header`}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', width: '100%', justifyContent: 'space-between' }}>
              <Typography>Compute Function - {computeFn} ({outputs.length} outputs)</Typography>
              <Box 
                sx={{ display: 'flex', alignItems: 'center' }}
                onClick={(e) => e.stopPropagation()}
              >
                <TextField
                  size="small"
                  placeholder="Search by ID"
                  value={searchTerms[computeFn] || ''}
                  onChange={(e) => setSearchTerms(prev => ({ ...prev, [computeFn]: e.target.value }))}
                  InputProps={{
                    endAdornment: (
                      <InputAdornment position="end">
                        <SearchIcon />
                      </InputAdornment>
                    ),
                  }}
                />
              </Box>
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}} elevation={0}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Created At</TableCell>
                    <TableCell>Output</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {outputs.map((output, idx) => (
                    <TableRow key={idx}>
                      <TableCell>{output.id}</TableCell>
                      <TableCell>{formatTimestamp(output.created_at)}</TableCell>
                      <TableCell>
                        <Button
                          onClick={() => fetchFunctionOutputPayload(computeFn, output.id)}
                        >
                        Download output  
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
}

export default InvocationOutputTable;
