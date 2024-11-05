import { useState } from 'react'
import { Alert, Card, CardContent, Grid, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Cpu, InfoCircle } from 'iconsax-react'
import DeleteIcon from '@mui/icons-material/Delete'
import { Link } from 'react-router-dom'
import { IndexifyClient } from 'getindexify'
import { ComputeGraph, ComputeGraphsList } from '../../types'
import CopyText from '../CopyText'
import TruncatedText from '../TruncatedText'

interface ComputeGraphsCardProps {
  client: IndexifyClient
  computeGraphs: ComputeGraphsList
  namespace: string
}

export function ComputeGraphsCard({ client, computeGraphs, namespace }: ComputeGraphsCardProps) {
  const [localGraphs, setLocalGraphs] = useState<ComputeGraph[]>(computeGraphs.compute_graphs || [])
  const [error, setError] = useState<string | null>(null)

  async function handleDeleteGraph(graphName: string) {
    try {
      await client.deleteComputeGraph(graphName)
      setLocalGraphs(prevGraphs => prevGraphs.filter(graph => graph.name !== graphName))
    } catch (err) {
      console.error('Error deleting compute graph:', err)
      setError('Failed to delete compute graph. Please try again.')
    }
  }

  function renderGraphCard(graph: ComputeGraph) {
    return (
      <Grid item xs={12} sm={6} md={4} key={graph.name} mb={2}>
        <Card sx={{ minWidth: 275, height: '100%', boxShadow: '0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset' }}>
          <CardContent>
            <Box display="flex" justifyContent="space-between">
              <Link to={`/${namespace}/compute-graphs/${graph.name}`}>
                <TruncatedText text={graph.name} maxLength={20} />
              </Link>
              <Box display="flex" flexDirection="row">
                <CopyText text={graph.name} />
                <IconButton 
                  onClick={() => handleDeleteGraph(graph.name)} 
                  aria-label="delete compute graph"
                >
                  <DeleteIcon color="error" />
                </IconButton>
              </Box>
            </Box>
            <Typography variant="subtitle2" color="text.secondary">
              Version: {graph.version || 'N/A'}
            </Typography>
            <Typography variant="subtitle2" color="text.secondary">
              Namespace: {graph.namespace}
            </Typography>
            <Typography variant="subtitle2" color="text.secondary">
              Number of Nodes: {Object.keys(graph.nodes || {}).length}
            </Typography>
          </CardContent>
        </Card>
      </Grid>
    )
  }

  function renderContent() {
    if (error) 
      return <Alert variant="outlined" severity="error" sx={{ my: 2 }}>{error}</Alert>

    if (!localGraphs.length)
      return <Alert variant="outlined" severity="info" sx={{ my: 2 }}>No Graphs Found</Alert>

    const sortedGraphs = [...localGraphs].sort((a, b) => a.name.localeCompare(b.name))

    return (
      <Box sx={{ width: '100%', overflow: 'auto', borderRadius: '5px' }} mt={2}>
        <Grid container spacing={1}>
          {sortedGraphs.map(renderGraphCard)}
        </Grid>
      </Box>
    )
  }

  return (
    <>
      <Stack direction="row" alignItems="center" spacing={2}>
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
  )
}
