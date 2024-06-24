import { useLoaderData } from 'react-router-dom'
import { Typography, Stack, Breadcrumbs } from '@mui/material'
import {
  IContentMetadata,
  IExtractedMetadata,
  IndexifyClient,
  ITask,
} from 'getindexify'
import { useEffect, useState } from 'react'
import TasksTable from '../../components/TasksTable'
import { Link } from 'react-router-dom'
import ExtractedMetadataTable from '../../components/tables/ExtractedMetaDataTable'
import Errors from '../../components/Errors'
import {
  formatBytes,
} from '../../utils/helpers'
import moment from 'moment'
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import DetailedContent from '../../components/DetailedContent'

const IndividualContentPage = () => {
  const {
    client,
    namespace,
    contentId,
    contentMetadata,
    groupedExtractedMetadata,
    errors,
  } = useLoaderData() as {
    namespace: string
    tasks: ITask[]
    contentId: string
    contentMetadata: IContentMetadata
    groupedExtractedMetadata: Record<string, IExtractedMetadata[]>
    client: IndexifyClient
    errors: string[]
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

  const taskLoader = async (
    pageSize: number,
    startId?: string
  ): Promise<ITask[]> => {
    const tasks = await client.getTasks({
      contentId,
      limit: pageSize + 1,
      startId,
    })
    return tasks
  }

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb" separator={<NavigateNextIcon fontSize="small" />}>
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/content`}>
          <Typography color="text.primary">Content</Typography>
        </Link>
        <Typography color="text.primary">{contentId}</Typography>
      </Breadcrumbs>
      <Errors errors={errors} />
      <Typography variant="h2">{contentId}</Typography>
      <DetailedContent
        filename={contentMetadata.name}
        source={contentMetadata.source}
        size={formatBytes(contentMetadata.size)}
        createdAt={moment(contentMetadata.created_at * 1000).format()}
        storageURL={contentMetadata.storage_url}
        parentID={contentMetadata.parent_id}
        namespace={namespace}
        mimeType={contentMetadata.mime_type}
        contentUrl={contentMetadata.content_url}
        textContent={textContent}
      />
      {/* tasks */}
      {Object.keys(groupedExtractedMetadata).map((key) => {
        const extractedMetadata = groupedExtractedMetadata[key]
        return (
          <ExtractedMetadataTable
            key={key}
            extractedMetadata={extractedMetadata}
          />
        )
      })}
      <TasksTable
        namespace={namespace}
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
        loadData={taskLoader}
        hideContentId
      />
    </Stack>
  )
}

export default IndividualContentPage;
