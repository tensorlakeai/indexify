import { useMemo } from 'react';
import {
  Box,
  Breadcrumbs,
  Typography,
  Stack,
  Alert,
} from '@mui/material';
import ExtendedContentTable from '../../components/ExtendedContentTable';
import { TableDocument } from 'iconsax-react';
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Link, useLoaderData } from 'react-router-dom';
import { ExtractionGraph, Extractor, IContentMetadata, IndexifyClient } from 'getindexify';
import { mapExtractionPoliciesToRows } from '../../utils/helpers';
import ExtractorGraphTable from './ExtractorGraphTable';

const groupContentByGraphs = (contentList: IContentMetadata[] | undefined) => {
  if (!contentList || !Array.isArray(contentList) || contentList.length === 0) {
    return {};
  }

  if (contentList.length === 1) {
    console.log('true')
    const content = contentList[0];
    if (content && Array.isArray(content.extraction_graph_names)) {
      return content.extraction_graph_names.reduce((acc, graphName) => {
        acc[graphName] = [content];
        return acc;
      }, {} as Record<string, IContentMetadata[]>);
    }
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

const IndividualExtractionGraphPage = () => {
  const { tasks,
    extractorName,
    extractors,
    extractionGraph,
    contentList,
    namespace } =
    useLoaderData() as {
      extractorName: string
      namespace: string
      client: IndexifyClient
      extractionGraph: ExtractionGraph
      tasks: any,
      extractors: Extractor[],
      contentList: IContentMetadata[],
    }
    const groupedContent = useMemo(() => {
      return groupContentByGraphs(contentList)
  }, [contentList])

  console.log('tasks', tasks);

  const extractionGraphString = JSON.parse(JSON.stringify(extractionGraph));
  const extractorString = JSON.parse(JSON.stringify(extractors));  
  const mappedRows = mapExtractionPoliciesToRows(extractionGraphString, extractorString, extractorName, tasks);

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
            </Typography>
          </div>
          <ExtractorGraphTable rows={mappedRows} graphName={extractorName} />
        </Box>
        {groupedContent[extractorName] ? (
          <ExtendedContentTable
            content={contentList}
            extractionGraph={extractionGraph}
            graphName={extractorName}
            namespace={namespace}
          />
        ) : (
          <Alert severity="info">No content found</Alert>
        )}
      </Box>
    </Stack>
  );
};

export default IndividualExtractionGraphPage;
