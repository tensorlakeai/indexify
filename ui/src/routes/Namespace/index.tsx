import { IndexifyClient, Extractor, IIndex, ISchema } from 'getindexify'
import { useLoaderData, LoaderFunctionArgs } from 'react-router-dom'
import { Stack } from '@mui/material'
import ContentTable from '../../components/tables/ContentTable'
import React from 'react'
import ExtractionGraphs from '../../components/ExtractionGraphs'
import ExtractorsTable from '../../components/tables/ExtractorsTable'
import { getIndexifyServiceURL } from '../../utils/helpers'
import SchemasTable from '../../components/tables/SchemasTable'
import IndexTable from '../../components/tables/IndexTable'
import { IContentMetadataExtended } from '../../types'

export async function loader({ params }: LoaderFunctionArgs) {
  const { namespace } = params
  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  })
  const [extractors, indexes, contentList, schemas] = await Promise.all([
    client.extractors(),
    client.indexes(),
    client.getExtractedContent(),
    client.getSchemas(),
  ])
  return {
    client,
    extractors,
    indexes,
    contentList,
    schemas,
    namespace,
  }
}

const NamespacePage = () => {
  const { client, extractors, indexes, schemas, namespace } =
    useLoaderData() as {
      client: IndexifyClient
      extractors: Extractor[]
      indexes: IIndex[]
      schemas: ISchema[]
      namespace: string
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
    <Stack direction="column" spacing={3}>
      <ExtractionGraphs
        namespace={client.namespace}
        extractionGraphs={client.extractionGraphs}
        extractors={extractors}
        tasks={[]}
      />
      <IndexTable
        namespace={namespace}
        indexes={indexes}
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
      />
      <SchemasTable schemas={schemas} />
      <ContentTable
        loadData={contentLoader}
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
      />
      <ExtractorsTable extractors={extractors} />
    </Stack>
  )
}

export default NamespacePage
