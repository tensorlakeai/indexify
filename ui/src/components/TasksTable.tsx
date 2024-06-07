import React from 'react'
import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { IExtractionPolicy, ITask } from 'getindexify'
import { Alert, Typography, useTheme } from '@mui/material'
import { Box, Stack } from '@mui/system'
import TaskIcon from '@mui/icons-material/Task'
import moment from 'moment'
import { Link } from 'react-router-dom'
import { TaskStatus } from 'getindexify'
import TaskCounts from './TaskCounts'
import HourglassBottomIcon from '@mui/icons-material/HourglassBottom'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ReportIcon from '@mui/icons-material/Report'

const TasksTable = ({
  namespace,
  policies,
  tasks,
  hideContentId,
  hideExtractionPolicy,
}: {
  namespace: string
  policies: IExtractionPolicy[]
  tasks: ITask[]
  hideContentId?: boolean
  hideExtractionPolicy?: boolean
}) => {
  const theme = useTheme()
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
        const policy = policies.find((policy) => policy.id === params.value)
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
      renderCell: (params) => {
        let text = ''
        if (params.value === TaskStatus.Failure) {
          return (
            <Box
              display="flex"
              alignItems="center"
              sx={{ color: theme.palette.error.main }}
            >
              <HourglassBottomIcon sx={{ width: 15 }} /> Failure
            </Box>
          )
        } else if (params.value === TaskStatus.Success) {
          return (
            <Box
              display="flex"
              alignItems="center"
              sx={{ color: theme.palette.success.main }}
              gap={0.5}
            >
              <CheckCircleIcon sx={{ width: 15 }} /> Success
            </Box>
          )
        } else {
          return (
            <Box
              display="flex"
              alignItems="center"
              sx={{ color: theme.palette.common.black }}
              gap={0.5}
            >
              <CheckCircleIcon sx={{ width: 15 }} /> Success
            </Box>
          )
        }
      },
      width: 100,
    },
    {
      field: 'content_metadata.source',
      headerName: 'Source',
      valueGetter: (params) =>
        params.row.content_metadata.source || 'Ingestion',
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
          rows={tasks}
          columns={columns}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 20 },
            },
          }}
          pageSizeOptions={[20, 50]}
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
        <Typography variant="h3">Tasks</Typography>{' '}
        <TaskCounts tasks={tasks} size="large" />
      </Stack>
      {renderContent()}
    </>
  )
}

export default TasksTable
