import {
  Box,
  Breadcrumbs,
  Typography,
  Stack,
} from '@mui/material';
// import ExtendedContentTable from '../../components/ExtendedContentTable';
import { TableDocument } from 'iconsax-react';
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData } from 'react-router-dom';
import { ComputeGraph, DataObject } from 'getindexify';
import ComputeGraphTable from './ComputeGraphTable';
import CopyText from '../../components/CopyText';
import InvocationsTable from '../../components/InvocationsTable';
import { useState } from 'react';

const IndividualComputeGraphPage = () => {
  const { 
    invocationsList,
    computeGraph,
    namespace
   } =
    useLoaderData() as {
      invocationsList: DataObject[],
      computeGraph: ComputeGraph
      namespace: string
    }
  const [invocations, setInvocations] = useState<DataObject[]>(invocationsList);
  const handleDelete = (updatedList: DataObject[]) => {
    setInvocations(updatedList);
  };

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/compute-graphs`}>
          <Typography color="text.primary">Compute Graphs</Typography>
        </Link>
        <Typography color="text.primary">{computeGraph.name}</Typography>
      </Breadcrumbs>
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument size="25" className="heading-icons" variant="Outline"/>
            </div>
            <Typography variant="h4" display={'flex'} flexDirection={'row'}>
              {computeGraph.name} <CopyText text={computeGraph.name} />
            </Typography>
          </div>
          <ComputeGraphTable namespace={namespace} graphData={computeGraph} />
        </Box>
        <InvocationsTable invocationsList={invocations} namespace={namespace} computeGraph={computeGraph.name} onDelete={handleDelete} />
      </Box>
    </Stack>
  );
};

export default IndividualComputeGraphPage;