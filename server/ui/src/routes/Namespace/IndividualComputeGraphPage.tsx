import { Box, Breadcrumbs, Typography, Stack, Chip } from '@mui/material';
import { TableDocument } from 'iconsax-react';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import { Link, useLoaderData } from 'react-router-dom';
import { useState, useCallback } from 'react';
import type { DataObject } from '../../types';
import type { IndividualComputeGraphLoaderData } from './types';
import ComputeGraphTable from '../../components/tables/ComputeGraphTable';
import CopyText from '../../components/CopyText';
import InvocationsTable from '../../components/tables/InvocationsTable';

const IndividualComputeGraphPage = () => {
  const { invocationsList, computeGraph, namespace } = 
    useLoaderData() as IndividualComputeGraphLoaderData;
    
  const [invocations, setInvocations] = useState<DataObject[]>(invocationsList);
  
  const handleDelete = useCallback((updatedList: DataObject[]) => {
    setInvocations(updatedList);
  }, []);

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link to={`/${namespace}/compute-graphs`}>
          <Typography color="text.primary">Compute Graphs</Typography>
        </Link>
        <Typography color="text.primary">{computeGraph.name}</Typography>
      </Breadcrumbs>
      
      <Box>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument 
                size="25" 
                className="heading-icons" 
                variant="Outline"
              />
            </div>
            <Typography 
              variant="h4" 
              sx={{ 
                display: 'flex', 
                flexDirection: 'row', 
                alignItems: 'center', 
                gap: 1 
              }}
            >
              {computeGraph.name}
              <Chip 
                label={`Version ${computeGraph.version}`} 
                size="small" 
              />
              <CopyText text={computeGraph.name} />
            </Typography>
          </div>
          
          <ComputeGraphTable 
            namespace={namespace} 
            graphData={computeGraph} 
          />
        </Box>
        
        <InvocationsTable 
          invocationsList={invocations}
          namespace={namespace}
          computeGraph={computeGraph.name}
          onDelete={handleDelete}
        />
      </Box>
    </Stack>
  );
};

export default IndividualComputeGraphPage;
