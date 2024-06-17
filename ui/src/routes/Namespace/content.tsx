import { Box } from '@mui/material'
import { IndexifyClient } from 'getindexify'
import { LoaderFunctionArgs, useLoaderData } from 'react-router-dom'
import { getIndexifyServiceURL } from '../../utils/helpers'
import ContentTable from '../../components/tables/ContentTable'
import { IContentMetadataExtended } from '../../types'

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })

  return { client }
}

const ContentsPage = () => {
  const { client } = useLoaderData() as {
    client: IndexifyClient
  }

  const contentLoader = async ({
    parentId,
    startId,
    pageSize,
  }: {
    parentId?: string
    startId?: string
    pageSize: number
  }): Promise<IContentMetadataExtended[]> => {
    const contentList = await client.getExtractedContent({
      parentId,
      startId,
      limit: pageSize + 1,
    })

    //count children
    return Promise.all(
      contentList.map(async (content) => {
        const tree = await client.getContentTree(content.id)
        return {
          ...content,
          children: tree.filter((c) => c.parent_id === content.id).length,
        }
      })
    )
  }

  return (
    <Box>
      <ContentTable
        loadData={contentLoader}
        client={client}
      />
    </Box>
  )
}

export default ContentsPage
