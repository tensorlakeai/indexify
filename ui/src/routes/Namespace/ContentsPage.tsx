import { Box } from '@mui/material'
import { ExtractionGraph, IndexifyClient } from 'getindexify'
import { useLoaderData } from 'react-router-dom'
import ContentTable from '../../components/tables/ContentTable'
import { IContentMetadataExtended } from '../../types'

const ContentsPage = () => {
  const { client, contentId, extractorName, policyName, extractionGraphs } = useLoaderData() as {
    contentId: string,
    extractorName: string;
    policyName: string,
    client: IndexifyClient
    extractionGraphs: ExtractionGraph[]
  }

  const contentLoader = async ({
    contentId,
    graphName,
    policyName
  }: {
    contentId: string
    graphName: string
    policyName: string
  }): Promise<IContentMetadataExtended[]> => {
    const { contentList } = await client.getExtractedContent({
      contentId,
      graphName,
      policyName,
    })

    return contentList
  }

  return (
    <Box>
      <ContentTable extractionGraphs={extractionGraphs} extractorName={extractorName} contentId={contentId} policyName={policyName} loadData={contentLoader} client={client} />
    </Box>
  )
}

export default ContentsPage
