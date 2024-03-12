import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Box } from "@mui/system";
import React from "react";
import { Stack } from "@mui/material";

const getRowId = (row: object) => {
  return JSON.stringify(row);
};

// attempt at autowidth
function createWidthMapFromObjects(
  dataArray: object[],
  averageCharWidth: number = 30,
  padding: number = 10
): { [key: string]: number } {
  const widthMap: { [key: string]: number } = {};
  //iterate rows
  dataArray.forEach((row) => {
    Object.entries(row).forEach(([key, value]) => {
      // get suggested width
      const currentWidth = String(value).length * averageCharWidth + padding;
      // update width map to max
      if (!widthMap[key] || currentWidth > widthMap[key]) {
        widthMap[key] = currentWidth;
      }
    });
  });

  return widthMap;
}

const ExtractedMetadataTable = ({ metadata }: { metadata: object[] }) => {
  const widthMap = createWidthMapFromObjects(metadata);
  console.log(widthMap);
  const columns: GridColDef[] = Object.keys(metadata[0]).map((key) => {
    return {
      field: key,
      headerName: key,
      width: widthMap[key],
      renderCell: (params) => {
        return (
          <Box sx={{ overflowX: "scroll" }}>
            <Stack gap={1} direction="row">
              {JSON.stringify(params.value)}
            </Stack>
          </Box>
        );
      },
    };
  });

  if (metadata.length === 0) {
    return null;
  }
  return (
    <Box
      sx={{
        width: "100%",
      }}
    >
      <DataGrid
        sx={{ backgroundColor: "white" }}
        autoHeight
        getRowId={getRowId}
        rows={metadata}
        columns={columns}
        initialState={{
          pagination: {
            paginationModel: { page: 0, pageSize: 5 },
          },
        }}
        pageSizeOptions={[5, 10]}
      />
    </Box>
  );
};

export default ExtractedMetadataTable;
