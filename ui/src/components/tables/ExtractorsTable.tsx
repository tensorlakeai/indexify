import React from 'react'
import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { Alert, Chip, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { Extractor } from 'getindexify'
import { Data, InfoCircle } from 'iconsax-react';

const ExtractorsTable = ({ extractors }: { extractors: Extractor[] }) => {
  const columns: GridColDef[] = [
    { field: 'name', headerName: 'Name', width: 300 },
    { field: 'description', headerName: 'Description', width: 300 },
    // {
    //   field: "input_mime_types",
    //   headerName: "Input MimeTypes",
    //   width: 300,
    //   renderCell: (params) => {
    //     return (
    //       <Box sx={{ overflowX: "scroll" }}>s
    //         <Stack gap={1} direction="row">
    //           {(params.value ?? []).map((val: string) => {
    //             return (
    //               <Chips
    //                 key={val}
    //                 label={val}
    //                 sx={{ backgroundColor: "#4AA4F4", color: "white" }}
    //               />
    //             );
    //           })}
    //         </Stack>
    //       </Box>
    //     );
    //   },
    // },
    {
      field: 'input_params',
      headerName: 'Input Parameters',
      width: 300,
      valueGetter: (params) => {
        return params.value?.properties
      },
      renderCell: (params) => {
        if (!params.value || Object.keys(params.value).length === 0) {
          return <Typography variant="body1">None</Typography>
        }
        return (
          <Box sx={{ overflowX: 'scroll' }} className="custom-scroll">
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val: string) => {
                return (
                  <Chip key={val} label={`${val}:${params.value[val].type}`} />
                )
              })}
            </Stack>
          </Box>
        )
      },
    },
    {
      field: 'outputs',
      headerName: 'Outputs',
      width: 300,
      valueGetter: (params) => {
        return params.value ?? {}
      },
      renderCell: (params) => {
        if (!params.value || Object.keys(params.value).length === 0) {
          return <Typography variant="body1">None</Typography>
        }
        return (
          <Box overflow="scroll">
            <Stack gap={1} direction="row" overflow="scroll">
              {Object.keys(params.value).map((val: string) => {
                return <Chip key={val} label={val} />
              })}
            </Stack>
          </Box>
        )
      },
    },
  ]

  const getRowId = (row: Extractor) => {
    return row.name
  }

  const renderContent = () => {
    if (extractors.length === 0) {
      return (
        <Box mt={2} mb={2}>
          <Alert variant="outlined" severity="info">
            No Extractors Found
          </Alert>
        </Box>
      )
    }
    return (
      <Box
        sx={{
          width: '100%',
          marginTop: '1rem',
        }}
      >
        <DataGrid
          sx={{ backgroundColor: 'white', borderRadius: '0.5rem' }}
          autoHeight
          getRowId={getRowId}
          rows={extractors}
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
          <Data size="25" className="heading-icons" variant="Outline"/>
        </div>
        <Typography variant="h4">
          Extractors
          <IconButton
            href="https://docs.getindexify.ai/concepts/#extractor"
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

export default ExtractorsTable
