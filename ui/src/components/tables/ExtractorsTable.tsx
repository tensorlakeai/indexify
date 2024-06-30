import React from 'react';
import { Alert, Chip, IconButton, Typography, Paper, Grid, ButtonBase } from '@mui/material';
import { Box, Stack } from '@mui/system';
import { Extractor } from 'getindexify';
import { Data, InfoCircle } from 'iconsax-react';
import { ScrollableChips } from '../Inputs/ScrollableChips';
import { TruncatedDescription } from '../Inputs/TruncatedDescription';

const ExtractorsTable = ({ extractors }: { extractors: Extractor[] }) => {
  const renderInputParams = (inputParams: any) => {
    if (!inputParams || Object.keys(inputParams).length === 0) {
      return <Chip label="None" sx={{ backgroundColor: '#E9EDF1', color: '#757A82' }} />;
    }
    return (
      <ScrollableChips inputParams={inputParams} />
    );
  };

  const renderOutputs = (outputs: any) => {
    if (!outputs || Object.keys(outputs).length === 0) {
      return <Chip label="None" sx={{ backgroundColor: '#E9EDF1', color: '#757A82' }} />;
    }
    return (
      <Box sx={{ overflowX: 'auto' }}>
        <Stack gap={1} direction="row">
          {Object.keys(outputs).map((val: string) => (
            <Chip key={val} label={val} variant="outlined" sx={{ border: '1px solid #6FA8EA' }} />
          ))}
        </Stack>
      </Box>
    );
  };

  const renderContent = () => {
    if (extractors.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Extractors Found
          </Alert>
        </Box>
      );
    }
    return (
      <Box sx={{ width: '100%', marginTop: '1rem' }}>
        <Grid container spacing={2}>
          {extractors.map((extractor) => (
            <Grid item xs={12} sm={6} md={4} lg={3} key={extractor.name}>
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
                    <ButtonBase sx={{ width: 16, height: 16 }}>
                      <Data size="16" variant="Outline"  />
                    </ButtonBase>
                  </Grid>
                  <Grid item xs>
                    <Typography gutterBottom variant="subtitle2" component="span">
                      {extractor.name}
                    </Typography>
                  </Grid>
                </Grid>
                <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                  Description: 
                </Typography>
                <TruncatedDescription description={extractor.description} />
                <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                  Input Parameters:
                </Typography>
                {renderInputParams(extractor.input_params?.properties)}
                <Box sx={{ flexGrow: 1 }} />
                <Typography variant="subtitle2" color="text.secondary" gutterBottom sx={{ mt: 2 }}>
                  Outputs:
                </Typography>
                {renderOutputs(extractor.outputs)}
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
          <Data size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Extractors
          <IconButton href="https://docs.getindexify.ai/concepts/#extractor" target="_blank">
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractorsTable;