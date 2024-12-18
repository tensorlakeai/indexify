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
import CopyTextPopover from '../../components/CopyTextPopover';

const IndividualInvocationPage = () => {
  const {
    indexifyServiceURL, 
    invocationId,
    computeGraph,
    namespace
   } =
    useLoaderData() as {
      indexifyServiceURL: string
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
        <CopyTextPopover text={namespace}>
          <Typography color="text.primary">{namespace}</Typography>
        </CopyTextPopover>
        <Link color="inherit" to={`/${namespace}/compute-graphs`}>
          <CopyTextPopover text="Compute Graphs">
            <Typography color="text.primary">Compute Graphs</Typography>
          </CopyTextPopover>
        </Link>
        <Link color="inherit" to={`/${namespace}/compute-graphs/${computeGraph}`}>
          <CopyTextPopover text={computeGraph}>
            <Typography color="text.primary">{computeGraph}</Typography>
          </CopyTextPopover>
        </Link>
        <CopyTextPopover text={invocationId}>
          <Typography color="text.primary">{invocationId}</Typography>
        </CopyTextPopover>
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
          <InvocationOutputTable indexifyServiceURL={indexifyServiceURL} invocationId={invocationId} namespace={namespace} computeGraph={computeGraph} />
        </Box>
        <InvocationTasksTable indexifyServiceURL={indexifyServiceURL} invocationId={invocationId} namespace={namespace} computeGraph={computeGraph} />
      </Box>
    </Stack>
  );
};

export default IndividualInvocationPage;
