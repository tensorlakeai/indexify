import { Alert, IconButton, Typography, Paper, Grid, Chip } from '@mui/material';
import { Box, Stack } from '@mui/system';
import { Setting4, InfoCircle } from 'iconsax-react';
import { ExecutorMetadata } from 'getindexify';

const ExecutorsCard = ({ executors }: { executors: ExecutorMetadata[] }) => {
  const renderContent = () => {
    if (executors || executors.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Executors Found
          </Alert>
        </Box>
      );
    }
    return (
      <Box sx={{ width: '100%', marginTop: '1rem' }}>
        <Grid container spacing={2}>
          {executors?.map((executor) => (
            <Grid item xs={12} sm={12} md={12} lg={12} key={executor.image_name}>
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
                      <Setting4 size="16" variant="Outline"  />
                    </div>
                  </Grid>
                  <Grid item xs>
                    <Typography gutterBottom variant="subtitle2" component="span">
                      {executor.image_name}
                    </Typography>
                  </Grid>
                </Grid>
                <Box sx={{display: 'flex', flexDirection: 'row'}}>
                  <Box sx={{ display: 'flex', alignItems: { xs: 'left', lg: 'flex-start' }, flexDirection: { xs: 'column', lg: 'row' }, mt: 1, mr: 2}}>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                      ID:&nbsp;
                    </Typography>
                    {executor.id}
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: { xs: 'left', lg: 'flex-start' }, flexDirection: { xs: 'column', lg: 'row' }, mt: 1, mr: 2 }}>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                      Address:&nbsp;
                    </Typography>
                    {executor.addr}
                  </Box>
                  <Box sx={{display: 'flex', flexDirection: 'row', alignItems: 'center'}}>
                    <Typography variant="subtitle2" className="text-gray-600 mr-2">
                      Labels: &nbsp;
                    </Typography>
                    {Object.entries(executor.labels).map(([key, value]) => (
                      <Chip
                        key={key}
                        label={`${key}: ${value}`}
                        variant="outlined"
                        size="small"
                        className="bg-blue-100 text-blue-800 border-blue-300"
                        sx={{mr: 1}}
                      />
                    ))}
                  </Box>
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
          <Setting4 size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Executors
          <IconButton href="https://docs.getindexify.ai/concepts/#executors" target="_blank">
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExecutorsCard;
