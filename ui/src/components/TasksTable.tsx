import React, { useEffect, useState } from 'react'
import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { IExtractionPolicy, ITask } from 'getindexify'
import { Alert, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import TaskIcon from '@mui/icons-material/Task'
import moment from 'moment'
import { Link } from 'react-router-dom'
import { TaskStatus } from 'getindexify'

const TasksTable = ({
  extractionPolicies,
  namespace,
  hideContentId,
  hideExtractionPolicy,
  loadData,
}: {
  extractionPolicies: IExtractionPolicy[]
  namespace: string
  loadData?: (pageSize: number, startId?: string) => Promise<ITask[]>
  hideContentId?: boolean
  hideExtractionPolicy?: boolean
}) => {
  const [rowCountState, setRowCountState] = useState(0)
  const [loading, setLoading] = useState(false)
  const [tasks, setTasks] = useState<ITask[]>([])
  const [startIds, setStartIds] = useState<Record<number, string>>({})
  const [paginationModel, setPaginationModel] = useState({
    page: 0,
    pageSize: 20,
  })

  useEffect(() => {
    let active = true

    ;(async () => {
      setLoading(true)
      if (!active || !loadData) return

      // load tasks for a given page
      const newTasks = await loadData(
        paginationModel.pageSize,
        paginationModel.page ? startIds[paginationModel.page - 1] : undefined
      )
      setTasks(newTasks)

      const newRowCount =
        paginationModel.page * paginationModel.pageSize + newTasks.length
      console.log('tasks count', newRowCount)
      setRowCountState(newRowCount)

      // add to startids if needed
      if (newTasks.length && startIds[paginationModel.page] === undefined) {
        const lastTaskId = newTasks[newTasks.length - 1].id
        setStartIds((prev) => ({
          ...prev,
          [paginationModel.page]: lastTaskId,
        }))
      }
      setLoading(false)
    })()

    return () => {
      active = false
    }
  }, [paginationModel])

  let columns: GridColDef[] = [
    {
      field: 'id',
      headerName: 'Task ID',
      width: 170,
    },
    {
      field: 'content_metadata.id',
      headerName: 'Content ID',
      width: 170,
      valueGetter: (params) => params.row.content_metadata.id,
      renderCell: (params) => (
        <Link to={`/${namespace}/content/${params.value}`}>{params.value}</Link>
      ),
    },
    {
      field: 'extraction_policy_id',
      headerName: 'Extraction Policy',
      renderCell: (params) => {
        const policy = extractionPolicies.find(
          (policy) => policy.id === params.value
        )
        return policy ? (
          <Link
            to={`/${namespace}/extraction-policies/${policy?.graph_name}/${policy?.name}`}
          >
            {policy?.name}
          </Link>
        ) : null
      },
      width: 200,
    },
    {
      field: 'outcome',
      headerName: 'Outcome',
      valueGetter: (params) => {
        if (params.value === TaskStatus.Failure) {
          return 'Failure'
        } else if (params.value === TaskStatus.Success) {
          return 'Success'
        } else {
          return 'Unknown'
        }
      },
      width: 100,
    },
    {
      field: 'content_metadata.source',
      headerName: 'Source',
      valueGetter: (params) => params.row.content_metadata.source,
      width: 170,
    },
    {
      field: 'content_metadata.created_at',
      headerName: 'Created At',
      renderCell: (params) => {
        return moment(params.row.content_metadata.created_at * 1000).format(
          'MM/DD/YYYY h:mm A'
        )
      },
      valueGetter: (params) => params.row.content_metadata.created_at,
      width: 200,
    },
  ]

  columns = columns.filter((col) => {
    if (hideContentId && col.field === 'content_metadata.id') {
      return false
    } else if (hideExtractionPolicy && col.field === 'extraction_policy_id') {
      return false
    }
    return true
  })

  const renderContent = () => {
    if (tasks.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Tasks Found
          </Alert>
        </Box>
      )
    }
    return (
      <Box
        sx={{
          width: '100%',
        }}
      >
        <DataGrid
          sx={{ backgroundColor: 'white' }}
          autoHeight
          rows={tasks.slice(0, paginationModel.pageSize)}
          rowCount={rowCountState}
          columns={columns}
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}
          paginationMode="server"
          loading={loading}
          pageSizeOptions={[20]}
        />
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
        <TaskIcon />
        <Typography variant="h3">Tasks</Typography>
      </Stack>
      {renderContent()}
    </>
  )
}

export default TasksTable
