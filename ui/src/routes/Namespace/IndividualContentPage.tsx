import React, { useEffect, useState } from 'react';
import { useLoaderData } from 'react-router-dom'
import { 
  Typography, 
  Stack, 
  Breadcrumbs, 
  Box, 
} from '@mui/material'
import {
  IContentMetadata,
  IndexifyClient,
  ExtractionGraph,
} from 'getindexify'
import { Link } from 'react-router-dom'
import { formatBytes } from '../../utils/helpers'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import DetailedContent from '../../components/DetailedContent'
import CopyText from '../../components/CopyText'
import PolicyContentTable from '../../components/tables/PolicyContentTable';

const IndividualContentPage = () => {
  const {
    client,
    namespace,
    contentId,
    contentMetadata,
    extractionGraphs,
    extractorName,
  } = useLoaderData() as {
    namespace: string
    contentId: string
    contentMetadata: IContentMetadata
    client: IndexifyClient
    extractionGraphs: ExtractionGraph[]
    extractorName: string
  }

  const [textContent, setTextContent] = useState('')

  useEffect(() => {
    if (
      contentMetadata.mime_type.startsWith('application/json') ||
      contentMetadata.mime_type.startsWith('text')
    ) {
      client.downloadContent<string | object>(contentId).then((data) => {
        if (typeof data === 'object') {
          setTextContent(JSON.stringify(data))
        } else {
          setTextContent(data)
        }
      })
    }
  }, [client, contentId, contentMetadata.mime_type])

  const currentExtractionGraph = extractionGraphs?.find(graph => graph.name === extractorName);

  return (
    <Stack direction="column" spacing={2}>
      <Breadcrumbs
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="small" />}
      >
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/extraction-graphs`}>
          <Typography color="text.primary">Extraction Graph</Typography>
        </Link>
        <Link color="inherit" to={`/${namespace}/extraction-graphs/${extractorName}`}>
          <Typography color="text.primary">{extractorName}</Typography>
        </Link>
        <Typography color="text.primary">{contentId}</Typography>
      </Breadcrumbs>
      <Box display="flex" flexDirection="row" alignItems="center">
        <Typography variant="h2">{contentId}</Typography>
        <CopyText text={contentId}/>
      </Box>
      <DetailedContent
        filename={contentMetadata.name}
        source={contentMetadata.source ? contentMetadata.source : "Ingestion"}
        size={formatBytes(contentMetadata.size)}
        createdAt={`${contentMetadata.created_at}`}
        storageURL={contentMetadata.storage_url}
        parentID={contentMetadata.parent_id}
        namespace={namespace}
        mimeType={contentMetadata.mime_type}
        contentUrl={`${contentMetadata.content_url}`}
        textContent={textContent}
        extractionGraph={extractorName}
      />
      
      {currentExtractionGraph && currentExtractionGraph.extraction_policies.map((policy) => (
        <Box key={policy.name} sx={{ marginTop: 10, p: 2, backgroundColor: "white",
        borderRadius: "0.5rem",
        boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset", }}>
          <Box display={'flex'} flexDirection={'row'}>
            <Typography variant="h6" sx={{ marginBottom: 2 }}>
              {policy.name}
            </Typography>
            <CopyText text={policy.name} />
          </Box>
          <PolicyContentTable 
            client={client}
            namespace={namespace}
            contentId={contentId}
            extractorName={extractorName}
            policyName={policy.name}
          />
        </Box>
      ))}
    </Stack>
  )
}

export default IndividualContentPage;
