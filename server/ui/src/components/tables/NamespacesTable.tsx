import { Alert, IconButton, Typography, Paper, Grid } from '@mui/material';
import { Box, Stack } from '@mui/system';
import { Namespace } from 'getindexify';
import { TableDocument, InfoCircle } from 'iconsax-react';
import { formatTimestamp } from '../../utils/helpers';

const NamespacesTable = ({ namespaces }: { namespaces: Namespace[] }) => {
  const renderContent = () => {
    if (namespaces.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Namespaces Found
          </Alert>
        </Box>
      );
    }
    return (
      <Box sx={{ width: '100%', marginTop: '1rem' }}>
        <Grid container spacing={2}>
          {namespaces.map((namespace) => (
            <Grid item xs={12} sm={12} md={12} lg={12} key={namespace.name}>
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
                    <div className="heading-icon-container">
                      <TableDocument size="16" variant="Outline"  />
                    </div>
                  </Grid>
                  <Grid item xs>
                    <Typography gutterBottom variant="subtitle2" component="span">
                      {namespace.name}
                    </Typography>
                  </Grid>
                </Grid>
                <Box sx={{ display: 'flex', alignItems: { xs: 'left', lg: 'flex-start' }, flexDirection: { xs: 'column', lg: 'row' }, mt: 1 }}>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Created At: 
                  </Typography>
                  {formatTimestamp(namespace.created_at)}
                </Box>
              </Paper>
            </Grid>
          ))}
        </Grid>
      </Box>
    );
  };

  return (
    <>
      <Stack display="flex" direction="row" alignItems="center" spacing={2}>
        <div className="heading-icon-container">
          <TableDocument size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Namespaces
          <IconButton href="https://docs.getindexify.ai/concepts/#namespaces" target="_blank">
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default NamespacesTable;
