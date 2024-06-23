import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { IExtractionPolicy, IIndex } from 'getindexify'
import { Alert, Button, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Link } from 'react-router-dom'
import CopyText from '../CopyText'
import { InfoCircle, MobileProgramming } from 'iconsax-react'

const IndexTable = ({
  indexes,
  namespace,
  extractionPolicies,
}: {
  indexes: IIndex[]
  namespace: string
  extractionPolicies: IExtractionPolicy[]
}) => {
  const getPolicyFromIndexname = (
    indexName: string
  ): IExtractionPolicy | undefined => {
    return extractionPolicies.find((policy) =>
      String(indexName).startsWith(`${policy.graph_name}.${policy.name}`)
    )
  }

  const columns: GridColDef[] = [
    {
      field: 'name',
      headerName: 'Name',
      flex: 2,
      renderCell: (params) => {
        return (
          <>
            {params.value}
            <CopyText text={params.value} className="show-onHover" />
          </>
        )
      },
    },
    {
      field: 'policy_name',
      headerName: 'Description',
      flex: 1,
      renderCell: (params) => {
        const policy = getPolicyFromIndexname(params.row.name)
        if (!policy) {
          return null
        }
        return (
          <Link
            to={`/${namespace}/extraction-policies/${policy.graph_name}/${policy.name}`}
          >
            {policy.name}
          </Link>
        )
      },
    },
    {
      field: 'searchIndex',
      headerName: 'Action',
      flex: 1,
      renderCell: (params) => {
        return (
          <>
            <Link
              to={`/${namespace}/indexes/${params.row.name}`}
            >
              <Button sx={{ py: 0.5, px: 2 }} variant="contained" color="primary">
                Search Index
              </Button>
            </Link>
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
            sx={{ backgroundColor: 'white', boxShadow: '0px 0px 2px 0px #D0D6DE' }}
            autoHeight
            getRowId={getRowId}
            rows={indexes}
            columns={columns}
            initialState={{
              pagination: {
                paginationModel: { page: 0, pageSize: 5 },
              },
            }}
            pageSizeOptions={[5, 10]}
            className='custom-data-grid'
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
        <div className='heading-icon-container'>
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

export default IndexTable
