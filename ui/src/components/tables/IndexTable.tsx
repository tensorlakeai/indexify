import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { IIndex } from 'getindexify'
import { Alert, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Link } from 'react-router-dom'
import CopyText from '../CopyText'
import { InfoCircle, MobileProgramming } from 'iconsax-react'

const IndexTable = ({
  indexes,
  namespace,
}: {
  indexes: IIndex[]
  namespace: string
}) => {

  const columns: GridColDef[] = [
    {
      field: 'name',
      headerName: 'Name',
      flex: 2,
      renderCell: (params) => {
        return (
          <>
            <Link
              to={`/${namespace}/indexes/${params.row.name}`}
            >
              {params.value}
            </Link>
            <CopyText text={params.value} className="show-onHover" />
          </>
        )
      },
    },
    {
      field: 'extractionGraphName',
      headerName: 'Extraction Graph Name',
      flex: 2,
      valueGetter: (params) => params.row.name,
      renderCell: (params) => {
        const extractionGraphName = params.value.split('.')[0] || '';
        return (
          <>
            <Link
              to={`/${namespace}/extraction-graphs/${extractionGraphName}`}
            >
              {extractionGraphName}
            </Link>
            <CopyText text={extractionGraphName} className="show-onHover" />
          </>
        )
      },
    },
  ]

  const getRowId = (row: IIndex) => {
    return row.name
  }

  const renderContent = () => {
    if (indexes.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Indexes Found
          </Alert>
        </Box>
      )
    }
    return (
      <>
        <Box
          style={{
            width: '100%',
            marginTop: '1rem',
          }}
        >
          <DataGrid
            sx={{ backgroundColor: 'white', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset", }}
            autoHeight
            getRowId={getRowId}
            rows={indexes}
            columns={columns}
            initialState={{
              pagination: {
                paginationModel: { page: 0, pageSize: 5 },
              },
            }}
            pageSizeOptions={[5, 10, 20]}
            className="custom-data-grid"
          />
        </Box>
      </>
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
        <div className="heading-icon-container">
          <MobileProgramming size="25" className="heading-icons" variant="Outline"/>
        </div>
        <Typography variant="h4">
          Indexes
          <IconButton
            href="https://docs.getindexify.ai/apis/retrieval/#vector-indexes"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline"/>
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  )
}

export default IndexTable;

