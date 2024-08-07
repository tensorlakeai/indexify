import { ExtractionGraph, IndexifyClient } from 'getindexify'
import { Alert, Card, CardContent, Grid, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import CopyText from './CopyText'
import { Cpu, InfoCircle } from 'iconsax-react'
import { Link } from 'react-router-dom'
import DeleteIcon from '@mui/icons-material/Delete';
import TruncatedText from './TruncatedText'

const ExtractionGraphs = ({
  client,
  extractionGraphs,
  namespace,
}: {
  client: IndexifyClient
  extractionGraphs: ExtractionGraph[]
  namespace: string
}) => {

  const handleDeleteExtractionGraph = async (extractionGraphName: string) => {
    try {
      await client.deleteExtractionGraph(namespace, extractionGraphName);
    } catch (error) {
      console.error("Error deleting content:", error);
    }
  };

  const renderContent = () => {
    if (extractionGraphs.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Graphs Found
          </Alert>
        </Box>
      )
    }

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
          {extractionGraphs.sort((a, b) => a.name.localeCompare(b.name)).map((graph)  => (
            <Grid item xs={12} sm={6} md={4} key={graph.name} mb={2}>
              <Card sx={{ minWidth: 275, height: '100%', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
                <CardContent>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Link to={`/${namespace}/extraction-graphs/${graph.name}`}>
                      <TruncatedText text={graph.name} maxLength={20} />
                    </Link>
                    <Box display="flex" flexDirection="row">
                      <CopyText text={graph.name} />
                      <IconButton onClick={() => handleDeleteExtractionGraph(graph.name)} aria-label="delete extraction graph">
                        <DeleteIcon color="error" />
                      </IconButton>
                    </Box>
                  </div>
                  <Typography variant="subtitle2" color="text.secondary">
                    Namespace: {graph.namespace}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary">
                    Number of Extractors: {graph.extraction_policies.length}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      </Box>
    )
  }

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
          Extraction Graphs
          <IconButton
            href="https://docs.getindexify.ai/concepts/#extraction-graphs"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  )
}

export default ExtractionGraphs
