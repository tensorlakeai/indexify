import {
  Box,
  Breadcrumbs,
  Typography,
  Stack,
} from '@mui/material';
import { TableDocument } from 'iconsax-react';
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData } from 'react-router-dom';
import CopyText from '../../components/CopyText';
import InvocationOutputTable from '../../components/tables/InvocationOutputTable';
import InvocationTasksTable from '../../components/tables/InvocationTasksTable';

const IndividualInvocationPage = () => {
  const { 
    invocationId,
    computeGraph,
    namespace
   } =
    useLoaderData() as {
      invocationId: string,
      computeGraph: string,
      namespace: string
    }

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
        <Link color="inherit" to={`/${namespace}/compute-graphs/${computeGraph}`}>
          <Typography color="text.primary">{computeGraph}</Typography>
        </Link>
        <Typography color="text.primary">{invocationId}</Typography>
      </Breadcrumbs>
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument size="25" className="heading-icons" variant="Outline"/>
            </div>
            <Typography variant="h4" display={'flex'} flexDirection={'row'}>
              Invocation - {invocationId} <CopyText text={invocationId} />
            </Typography>
          </div>
          <InvocationOutputTable invocationId={invocationId} namespace={namespace} computeGraph={computeGraph} />
        </Box>
        <InvocationTasksTable invocationId={invocationId} namespace={namespace} computeGraph={computeGraph} />
      </Box>
    </Stack>
  );
};

export default IndividualInvocationPage;
