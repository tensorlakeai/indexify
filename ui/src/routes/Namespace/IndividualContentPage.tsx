/* eslint-disable @typescript-eslint/no-unused-vars */
import { useLoaderData } from 'react-router-dom'
import { Typography, Stack, Breadcrumbs, Box, Tab, Tabs, TextField, FormControl, InputLabel, MenuItem, Select, Button, Alert } from '@mui/material'
import {
  IContentMetadata,
  IExtractedMetadata,
  IndexifyClient,
  ITask,
  ExtractionGraph,
} from 'getindexify'
import { useEffect, useState } from 'react'
import TasksTable from '../../components/TasksTable'
import { Link } from 'react-router-dom'
import ExtractedMetadataTable from '../../components/tables/ExtractedMetaDataTable'
import Errors from '../../components/Errors'
import { formatBytes } from '../../utils/helpers'
import moment from 'moment'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import DetailedContent from '../../components/DetailedContent'
import { DataGrid, GridColDef, GridRenderCellParams, GridValueGetterParams } from '@mui/x-data-grid'
import CopyText from '../../components/CopyText'

function getChildCountMap(contents: IContentMetadata[]): Record<string, number> {
  const childrenCountMap: Record<string, number> = {}
  contents.forEach((content) => {
    if (content.parent_id) {
      if (childrenCountMap[content.parent_id]) {
        childrenCountMap[content.parent_id]++
      } else {
        childrenCountMap[content.parent_id] = 1
      }
    }
  })

  const result: Record<string, number> = {}
  contents.forEach((content) => {
    result[content.id] = childrenCountMap[content.id] || 0
  })

  return result
}

const IndividualContentPage = () => {
  const {
    client,
    namespace,
    contentId,
    contentMetadata,
    extractionGraph,
    extractorName,
  } = useLoaderData() as {
    namespace: string
    contentId: string
    contentMetadata: IContentMetadata
    client: IndexifyClient
    extractionGraph: ExtractionGraph
    extractorName: string
  }

  const [textContent, setTextContent] = useState('')
  const [paginationModel, setPaginationModel] = useState({
    page: 0,
    pageSize: 5,
  })
  const [graphTabIds, setGraphTabIds] = useState<string[]>([])
  const [searchFilter, setSearchFilter] = useState<{
    contentId: string
    policyName: string
  }>({ contentId: '', policyName: 'Any' })
  const [filteredContent, setFilteredContent] = useState<IContentMetadata[]>([contentMetadata])
  const [currentTab, setCurrentTab] = useState('ingested')

  const childCountMap = getChildCountMap([contentMetadata])

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

  const onClickChildren = (selectedContent: IContentMetadata) => {
    setGraphTabIds([...graphTabIds, selectedContent.id])
    setCurrentTab(selectedContent.id)
  }

  const onChangeTab = (event: React.SyntheticEvent, selectedValue: string) => {
    setCurrentTab(selectedValue)
  }

  useEffect(() => {
    if (currentTab === 'search') {
      setPaginationModel({ ...paginationModel, page: 0 })
      const searchPolicy = extractionGraph.extraction_policies.find(
        (policy) => policy.name === searchFilter.policyName
      )
      setFilteredContent(
        [contentMetadata].filter((c) => {
          if (!c.id.startsWith(searchFilter.contentId)) return false
          if (searchFilter.policyName === 'Any') return true
          if (searchPolicy && c.source === searchPolicy.name) return true
          return false
        })
      )
    } else if (currentTab === 'ingested') {
      setGraphTabIds([])
      setFilteredContent([contentMetadata])
    } else {
      setGraphTabIds((currentIds) => {
        const index = currentIds.indexOf(currentTab)
        const newIds = [...currentIds]
        newIds.splice(index + 1)
        return newIds
      })
      // In this case, we would need to fetch child content for the current tab
      // This is just a placeholder, you'll need to implement the actual fetching logic
      setFilteredContent([])
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const columns: GridColDef[] = [
  {
    field: 'id',
    headerName: 'ID',
    width: 200,
    renderCell: (params: GridRenderCellParams) => (
      <>
        {params.value}
        <CopyText text={params.value as string} />
      </>
    ),
  },
  {
    field: 'childCount',
    headerName: 'Children',
    width: 140,
    valueGetter: (params: GridValueGetterParams) => childCountMap[params.row.id],
    renderCell: (params: GridRenderCellParams) => {
      const clickable =
        currentTab !== 'search' && childCountMap[params.row.id] !== 0
      return (
        <Button
          onClick={(e) => {
            e.stopPropagation()
            onClickChildren(params.row as IContentMetadata)
          }}
          sx={{
            pointerEvents: clickable ? 'auto' : 'none',
            textDecoration: clickable ? 'underline' : 'none',
          }}
          variant="text"
        >
          <Typography variant="body1">{params.value}</Typography>
        </Button>
      )
    },
  },
  {
    field: 'source',
    headerName: 'Source',
    width: 220,
  },
  {
    field: 'parent_id',
    headerName: 'Parent ID',
    width: 170,
  },
  {
    field: 'labels',
    headerName: 'Labels',
    width: 150,
    valueGetter: (params: GridValueGetterParams) => {
      return JSON.stringify(params.value)
    },
  },
  {
    field: 'created_at',
    headerName: 'Created At',
    width: 200,
    valueGetter: (params: GridValueGetterParams) => {
      return moment(params.value * 1000).format('MM/DD/YYYY h:mm A')
    },
  },
].filter((col) => {
  if (
    currentTab === 'ingested' &&
    (col.field === 'source' || col.field === 'parent_id')
  ) {
    return false
  }
  return true
})

  const renderContent = () => {
    if (filteredContent.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Content Found
          </Alert>
        </Box>
      )
    }

    return (
      <Box sx={{ width: '100%' }}>
        <DataGrid
          initialState={{
            sorting: {
              sortModel: [{ field: 'created_at', sort: 'desc' }],
            },
          }}
          sx={{ backgroundColor: 'white' }}
          autoHeight
          rows={filteredContent}
          columns={columns}
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}
          pageSizeOptions={[5, 10, 20]}
        />
      </Box>
    )
  }

  return (
    <Stack direction="column" spacing={1}>
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
        contentUrl={`${contentMetadata.content_url}`}
        textContent={textContent}
      />
      {/* <Box>
        <Box justifyContent={'space-between'} display={'flex'}>
          <Tabs
            value={currentTab}
            onChange={onChangeTab}
            aria-label="content tabs"
          >
            <Tab value={'search'} label="Search" />
            <Tab value={'ingested'} label="Ingested" />
            {graphTabIds.map((id) => (
              <Tab key={`filter-${id}`} value={id} label={id} />
            ))}
          </Tabs>
          {currentTab === 'search' && (
            <Box display="flex" gap={2}>
              <TextField
                onChange={(e) =>
                  setSearchFilter({
                    ...searchFilter,
                    contentId: e.target.value,
                  })
                }
                value={searchFilter.contentId}
                label="Content Id"
                sx={{ width: 'auto' }}
                size="small"
              />
              <FormControl sx={{ minWidth: 200 }} size="small">
                <InputLabel id="extraction-policy-label">
                  Extraction Policy
                </InputLabel>
                <Select
                  labelId="extraction-policy-label"
                  id="extraction-policy-select"
                  label="Extraction Policy"
                  value={searchFilter.policyName}
                  onChange={(e) =>
                    setSearchFilter({
                      ...searchFilter,
                      policyName: e.target.value,
                    })
                  }
                >
                  <MenuItem value="Any">Any</MenuItem>
                  {extractionGraph.extraction_policies.map((policy) => (
                    <MenuItem key={policy.name} value={policy.name}>
                      {policy.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Box>
          )}
        </Box>
        {renderContent()}
      </Box> */}
      {/* <ExtractedMetadataTable
        extractedMetadata={contentMetadata}
      /> */}
      {/* <TasksTable
        namespace={namespace}
        extractionPolicies={client.extractionGraphs
          .map((graph) => graph.extraction_policies)
          .flat()}
        loadData={taskLoader}
        hideContentId
      /> */}
    </Stack>
  )
}

export default IndividualContentPage