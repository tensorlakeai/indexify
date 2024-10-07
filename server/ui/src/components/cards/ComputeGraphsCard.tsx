import { useState, useEffect } from 'react';
import { IndexifyClient } from 'getindexify'
import { Alert, Card, CardContent, Grid, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import CopyText from '../CopyText'
import { Cpu, InfoCircle } from 'iconsax-react'
import { Link } from 'react-router-dom'
import DeleteIcon from '@mui/icons-material/Delete';
import TruncatedText from '../TruncatedText'
import { ComputeGraph, ComputeGraphsList } from '../../types';

const ComputeGraphsCard = ({
  client,
  computeGraphs,
  namespace
}: {
  client: IndexifyClient
  computeGraphs: ComputeGraphsList
  namespace: string
}) => {
  const [localComputeGraphs, setLocalComputeGraphs] = useState<ComputeGraphsList>(computeGraphs);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLocalComputeGraphs(computeGraphs);
  }, [computeGraphs, namespace]);

  const handleDeleteComputeGraph = async (computeGraphName: string) => {
    try {
      await client.deleteComputeGraph(computeGraphName);
      setLocalComputeGraphs(prevGraphs => ({
        compute_graphs: prevGraphs.compute_graphs?.filter(graph => graph.name !== computeGraphName) || []
      }));
    } catch (error) {
      console.error("Error deleting compute graph:", error);
      setError('Failed to delete compute graph. Please try again.');
    }
  };

  const renderContent = () => {
    if (error) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="error">
            {error}
          </Alert>
        </Box>
      );
    }

    if (!localComputeGraphs.compute_graphs || localComputeGraphs.compute_graphs.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Graphs Found
          </Alert>
        </Box>
      );
    }

    const sortedGraphs = [...localComputeGraphs.compute_graphs].sort((a, b) => a.name.localeCompare(b.name));

    return (
      <Box
        sx={{
          width: '100%',
          overflow: 'auto',
          borderRadius: '5px',
        }}
        mt={2}
      >
        <Grid container spacing={1}>
          {sortedGraphs.map((graph: ComputeGraph)  => (
            <Grid item xs={12} sm={6} md={4} key={graph.name} mb={2}>
              <Card sx={{ minWidth: 275, height: '100%', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
                <CardContent>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Link to={`/${namespace}/compute-graphs/${graph.name}`}>
                      <TruncatedText text={graph.name} maxLength={20} />
                    </Link>
                    <Box display="flex" flexDirection="row">
                      <CopyText text={graph.name} />
                      <IconButton onClick={() => handleDeleteComputeGraph(graph.name)} aria-label="delete compute graph">
                        <DeleteIcon color="error" />
                      </IconButton>
                    </Box>
                  </div>
                  <Typography variant="subtitle2" color="text.secondary">
                    Namespace: {graph.namespace}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary">
                    Number of Nodes: {Object.keys(graph.nodes || {}).length}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      </Box>
    );
  };

  return (
    <>
      <Stack
        display={'flex'}
        direction={'row'}
        alignItems={'center'}
        spacing={2}
      >
        <div className="heading-icon-container">
          <Cpu size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Compute Graphs
          <IconButton
            href="https://docs.getindexify.ai/concepts/#compute-graphs"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ComputeGraphsCard;
