/* eslint-disable @typescript-eslint/no-unused-vars */
import { ExtractionGraph, IExtractionPolicy, IExtractor } from 'getindexify'
import { Alert, Card, CardContent, Grid, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { ReactElement } from 'react'
import ExtractionPolicyItem from './ExtractionPolicyItem'
import {
  IExtractionGraphCol,
  IExtractionGraphColumns,
  TaskCountsMap,
} from '../types'
import CopyText from './CopyText'
import { Cpu, InfoCircle } from 'iconsax-react'
import ExtractionPolicyTable from './ExtractionPolicyTable'
import ExtendedContentTable from './ExtendedContentTable'
import { Link } from 'react-router-dom'

const ExtractionGraphs = ({
  extractionGraphs,
  namespace,
  extractors,
  taskCountsMap,
}: {
  extractionGraphs: ExtractionGraph[]
  namespace: string
  extractors: IExtractor[]
  taskCountsMap: TaskCountsMap
}) => {
  const itemheight = 60
  const cols: IExtractionGraphColumns = {
    name: { displayName: 'Name', width: 350 },
    extractor: { displayName: 'Extractor', width: 225 },
    mimeTypes: { displayName: 'Input MimeTypes', width: 225 },
    inputParams: { displayName: 'Input Parameters', width: 225 },
    taskCount: { displayName: 'Tasks', width: 75 },
  }

  const renderHeader = () => {
    return (
      <Stack
        direction={'row'}
        px={2}
        py={2}
        sx={{
          width: '100%',
          borderBottom: '1px solid #e5e5e5',
        }}
      >
        {Object.values(cols).map((col: IExtractionGraphCol) => {
          return (
            <Box key={col.displayName} minWidth={`${col.width}px`}>
              <Typography variant="label">{col.displayName}</Typography>
            </Box>
          )
        })}
      </Stack>
    )
  }

  const renderGraphItems = (
    policies: IExtractionPolicy[],
    source: string,
    depth = 0
  ): ReactElement[] => {
    let items: ReactElement[] = []
    // use sibling count to keep track of how many are above
    let siblingCount = items.length
    console.log('policies', policies)
    policies
      .filter((policy) => policy.content_source === source)
      .forEach((policy, i) => {
        items.push(
          <ExtractionPolicyItem
            key={policy.name}
            taskCounts={taskCountsMap.get(policy.id!)!}
            extractionPolicy={policy}
            namespace={namespace}
            cols={cols}
            extractors={extractors}
            depth={depth}
            siblingCount={siblingCount}
            itemHeight={itemheight}
          />
        )
        const children = renderGraphItems(policies, policy.name, depth + 1)
        items = items.concat(children)
        siblingCount = children.length
      })
    return items
  }

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
          {extractionGraphs.map((graph)  => (
            <Grid item xs={12} sm={6} md={4} key={graph.name} mb={2}>
              <Card sx={{ minWidth: 275, height: '100%', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
                <CardContent>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Link to={`/${namespace}/extraction-graphs/${graph.name}`}>
                      <Typography variant="h6" component="div">
                        {graph.name}
                      </Typography>
                    </Link>
                    <CopyText text={graph.name} />
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
