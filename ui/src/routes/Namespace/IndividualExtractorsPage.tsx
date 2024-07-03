/* eslint-disable @typescript-eslint/no-unused-vars */
import React from 'react';
import {
  Box,
  Breadcrumbs,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  Chip,
  IconButton,
  Stack,
} from '@mui/material';
import ExtendedContentTable from '../../components/ExtendedContentTable';
import { InfoCircle, TableDocument } from 'iconsax-react';
import CopyText from '../../components/CopyText';
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData } from 'react-router-dom';
import { ExtractionGraph, Extractor, IContentMetadata, IExtractionPolicy, IIndex, IndexifyClient, ISchema } from 'getindexify';
import { TaskCounts, TaskCountsMap } from '../../types';

const ExtractorTable = () => {
  const rows = [
    { id: 1, name: 'moondreamextractor', extractor: 'tensorlake/moondream', inputTypes: ['image/bmp', 'image/gif'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 2, name: 'minilm-moondream-caption', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 3, name: 'minilm-moondream-caption-efefegeef...', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
    { id: 4, name: 'minilm-moondream-wordsege-plentyofroom', extractor: 'tensorlake/moondream', inputTypes: ['text plain'], inputParameters: 'None', pending: 20, failed: 12, completed: 24 },
  ];

  return (
    <TableContainer component={Paper} sx={{borderRadius: '8px', mt: 2, boxShadow: 'none'}}>
      <Table sx={{ minWidth: 650 }} aria-label="extractor table">
        <TableHead>
          <Typography variant="h4" sx={{ display: 'flex', alignItems: 'center', ml: 2, mt: 2 }}>
            moondreamcaptionkb
            <CopyText text='moondreamcaptionkb' />
          </Typography>
          <TableRow>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Name</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Extractor</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Input Types</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Input Parameters</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Pending</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Failed</TableCell>
            <TableCell sx={{ fontSize: 14, pt: 1}}>Completed</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.id}>
              <TableCell sx={{ pt: 2}}>
                <Box sx={{ display: 'flex', alignItems: 'left', flexDirection: 'column' }}>
                  <Box sx={{ mr: 1, display: 'flex' }}>
                    {Array(row.id).fill(0).map((_, i) => (
                      <Box key={i} sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#6FA8EA', mr: 0.5 }} />
                    ))}
                  </Box>
                  {row.name}
                </Box>
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.extractor}</TableCell>
              <TableCell sx={{ pt: 2}}>
                {row.inputTypes.map((type, index) => (
                  <Chip key={index} label={type} size="small" sx={{ mr: 0.5 }} />
                ))}
              </TableCell>
              <TableCell sx={{ pt: 2}}>{row.inputParameters}</TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.pending} sx={{ backgroundColor: '#E5EFFB' }} /></TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.failed} sx={{ backgroundColor: '#FBE5E5' }} /></TableCell>
              <TableCell sx={{ pt: 2}}><Chip label={row.completed} sx={{ backgroundColor: '#E5FBE6' }} /></TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

const IndividualExtractorsPage = () => {
  const { getTasks,
    taskCountsMap,
    client,
    extractors,
    extractionGraph,
    indexes,
    contentList,
    schemas,
    namespace } =
    useLoaderData() as {
      namespace: string
      client: IndexifyClient
      extractionGraph: ExtractionGraph
      taskCounts?: TaskCounts
      getTasks: any,
      taskCountsMap: TaskCountsMap,
      extractors: Extractor[],
      indexes: IIndex[],
      contentList: IContentMetadata[],
      schemas: ISchema[],
    }

  console.log('taskCountsMap', taskCountsMap)
  console.log('extractors', extractors)
  console.log('extractionGraph', extractionGraph)
  console.log('indexes', indexes)
  console.log('contentList', contentList)
  console.log('schemas', schemas)
  console.log('namespace', namespace)
  console.log('client', client)
  console.log('getTasks', getTasks)

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/extraction-graphs`}>
          <Typography color="text.primary">Extraction Graphs</Typography>
        </Link>
        <Typography color="text.primary">{extractionGraph?.name}</Typography>
      </Breadcrumbs>
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument size="25" className="heading-icons" variant="Outline"/>
            </div>
            <Typography variant="h4">
              tensorlake/moondream
              <IconButton
                href="https://docs.getindexify.ai/concepts/#content"
                target="_blank"
              >
                <InfoCircle size="20" variant="Outline"/>
              </IconButton>
            </Typography>
          </div>
          <ExtractorTable />
        </Box>
        <ExtendedContentTable />
      </Box>
    </Stack>
  );
};

export default IndividualExtractorsPage;