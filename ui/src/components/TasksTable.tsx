import { useEffect, useState } from 'react'
import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { IExtractionPolicy, ITask } from 'getindexify'
import { Alert, Chip } from '@mui/material'
import { Box } from '@mui/system'
import moment from 'moment'
import { Link } from 'react-router-dom'
import { TaskStatus } from 'getindexify'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';

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
      setRowCountState(newRowCount)

      // add to startids if needed
      if (newTasks.length && startIds[paginationModel.page] === undefined) {
        const lastId = newTasks[newTasks.length - 1].id
        setStartIds((prev) => ({
          ...prev,
          [paginationModel.page]: lastId,
        }))
      }
      setLoading(false)
    })()

    return () => {
      active = false
    }
  }, [paginationModel, loadData, startIds])

  let columns: GridColDef[] = [
    {
      field: 'id',
      headerName: 'Task ID',
      flex: 1,
    },
    {
      field: 'content_metadata.id',
      headerName: 'Content ID',
      flex: 1,
      valueGetter: (params) => params.row.content_metadata.id,
      renderCell: (params) => {
        const policy = extractionPolicies.find(
          (policy) => policy.id === params.value
        )
        return (<Link to={`/${namespace}/extraction-graphs/${policy?.name}/content/${params.value}`}>{params.value}</Link>);
    },
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
      flex: 1,
    },
    {
      field: 'outcome',
      flex: 1,
      headerName: 'Status',
      renderCell: (params) => {
        if (params.value === TaskStatus.Failure) {
          return <Chip icon={<ErrorOutlineIcon />} label="Failure" variant="outlined" color="error" sx={{ backgroundColor: '#FBE5E5' }} />
        } else if (params.value === TaskStatus.Success) {
          return <Chip icon={<CheckCircleOutlineIcon />} label="Success" variant="outlined" color="success" sx={{ backgroundColor: '#E5FBE6' }} />
        } else {
          return  (
            <Chip icon={<InfoOutlinedIcon />} label="In Progress" variant="outlined" color="info" sx={{ backgroundColor: '#E5EFFB' }}  />
          )
        }
      },
    },
    {
      field: 'content_metadata.source',
      headerName: 'Source',
      valueGetter: (params) =>
        params.row.content_metadata.source || 'Ingestion',
      flex: 1,
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
      flex: 1,
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
          sx={{ backgroundColor: 'white', borderRadius: '12px' }}
          autoHeight
          rows={tasks.slice(0, paginationModel.pageSize)}
          rowCount={rowCountState}
          columns={columns}
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}
          paginationMode="server"
          loading={loading}
          pageSizeOptions={[5, 10, 20]}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 5 }
            }
          }}
        />
      </Box>
    )
  }

  return <>{renderContent()}</>
}

export default TasksTable
