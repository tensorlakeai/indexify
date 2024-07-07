/* eslint-disable @typescript-eslint/no-unused-vars */
import React, { useMemo } from 'react';
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
  Alert,
} from '@mui/material';
import ExtendedContentTable from '../../components/ExtendedContentTable';
import { InfoCircle, TableDocument } from 'iconsax-react';
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData, useParams } from 'react-router-dom';
import { ExtractionGraph, Extractor, IContentMetadata, IExtractionPolicy, IIndex, IndexifyClient, ISchema } from 'getindexify';
import { TaskCounts, TaskCountsMap } from '../../types';
import { mapExtractionPoliciesToRows, Row } from '../../utils/helpers';
import ExtractorGraphTable from './ExtractorGraphTable';

const groupContentByGraphs = (contentList: IContentMetadata[] | undefined) => {
  console.log('contentList', contentList)
  if (!contentList || !Array.isArray(contentList) || contentList.length === 0) {
    return {};
  }

  return contentList.reduce((acc, content) => {
    if (content && Array.isArray(content.extraction_graph_names)) {
      content.extraction_graph_names.forEach(graphName => {
        if (!acc[graphName]) {
          acc[graphName] = [];
        }
        acc[graphName].push(content);
      });
    }
    return acc;
  }, {} as Record<string, IContentMetadata[]>);
};

const IndividualExtractorsPage = () => {
  
  const { getTasks,
    extractorName,
    taskCountsMap,
    client,
    extractors,
    extractionGraph,
    indexes,
    contentList,
    schemas,
    namespace } =
    useLoaderData() as {
      extractorName: string
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

    const groupedContent = useMemo(() => {
    return groupContentByGraphs(contentList)
  }, [contentList])

    console.log('taskCountsMap', taskCountsMap)
    console.log('getTasks', getTasks)
    console.log('extractors', extractors)
    console.log('extractionGraph', extractionGraph)

  const extractionGraphString = JSON.parse(JSON.stringify(extractionGraph)); // This could be an array or a single object
  const extractorString = JSON.parse(JSON.stringify(extractors));  
  const mappedRows = mapExtractionPoliciesToRows(extractionGraphString, extractorString, extractorName);

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
        <Typography color="text.primary">{extractorName}</Typography>
      </Breadcrumbs>
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3 }}>
          <div className="content-table-header">
            <div className="heading-icon-container">
              <TableDocument size="25" className="heading-icons" variant="Outline"/>
            </div>
            <Typography variant="h4">
              {extractorName}
              <IconButton
                href="https://docs.getindexify.ai/concepts/#content"
                target="_blank"
              >
                <InfoCircle size="20" variant="Outline"/>
              </IconButton>
            </Typography>
          </div>
          <ExtractorGraphTable rows={mappedRows} graphName={extractorName} />
        </Box>
        {groupedContent[extractorName] ? (
          <ExtendedContentTable
            content={groupedContent[extractorName]}
            extractionGraph={extractionGraph}
          />
        ) : (
          <Alert severity="info">No content found</Alert>
        )}
      </Box>
    </Stack>
  );
};

export default IndividualExtractorsPage;