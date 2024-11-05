import { Alert, IconButton, Typography, Paper, Grid } from '@mui/material';
import { Box, Stack } from '@mui/system';
import { TableDocument, InfoCircle } from 'iconsax-react';
import { formatTimestamp } from '../../utils/helpers';
import { Namespace } from '../../types';

interface NamespacesCardProps {
  namespaces: Namespace[];
}

function TableDocumentIcon({ size }: { size: string }) {
  return (
    <div className="heading-icon-container">
      <TableDocument size={size} variant="Outline" />
    </div>
  );
}

function NamespaceItem({ namespace }: { namespace: Namespace }) {
  return (
    <Grid item xs={12} key={namespace.name}>
      <Paper
        sx={{
          p: 2,
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          borderRadius: '8px',
          boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"
        }}
        elevation={0}
      >
        <Grid container spacing={2} alignItems="center" mb={1}>
          <Grid item>
            <TableDocumentIcon size="16" />
          </Grid>
          <Grid item xs>
            <Typography variant="subtitle2" component="span">
              {namespace.name}
            </Typography>
          </Grid>
        </Grid>
        <Box 
          sx={{ 
            display: 'flex', 
            alignItems: { xs: 'left', lg: 'flex-start' }, 
            flexDirection: { xs: 'column', lg: 'row' }, 
            mt: 1 
          }}
        >
          <Typography variant="subtitle2" color="text.secondary">
            Created At: &nbsp;
          </Typography>
          {formatTimestamp(namespace.created_at)}
        </Box>
      </Paper>
    </Grid>
  );
}

function NamespacesCard({ namespaces }: NamespacesCardProps) {
  if (namespaces.length === 0) 
    return (
      <Box mt={2} mb={2}>
        <Alert variant="outlined" severity="info">
          No Namespaces Found
        </Alert>
      </Box>
    );

  return (
    <>
      <Stack direction="row" alignItems="center" spacing={2}>
        <TableDocumentIcon size="25" />
        <Typography variant="h4">
          Namespaces
          <IconButton 
            href="https://docs.getindexify.ai/concepts/#namespaces" 
            target="_blank"
            aria-label="Learn more about namespaces"
          >
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      <Box sx={{ width: '100%', marginTop: '1rem' }}>
        <Grid container spacing={2}>
          {namespaces.map((namespace) => (
            <NamespaceItem namespace={namespace} key={namespace.name} />
          ))}
        </Grid>
      </Box>
    </>
  );
}

export default NamespacesCard;
