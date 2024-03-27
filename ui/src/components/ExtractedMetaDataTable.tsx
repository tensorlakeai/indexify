import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Box } from "@mui/system";
import React from "react";
import { Stack, Typography } from "@mui/material";
import { IExtractedMetadata } from "getindexify";

const getRowId = (row: object) => {
  return JSON.stringify(row);
};

// attempt at autowidth
function createWidthMapFromObjects(
  dataArray: object[],
  averageCharWidth: number = 10,
  padding: number = 10
): { [key: string]: number } {
  const widthMap: { [key: string]: number } = {};
  //iterate rows
  dataArray.forEach((row) => {
    Object.entries(row).forEach(([key, value]) => {
      // get suggested width
      const currentWidth =
        Math.max(JSON.stringify(value).length, key.length) * averageCharWidth + padding;
      // update width map to max
      if (!widthMap[key] || currentWidth > widthMap[key]) {
        widthMap[key] = currentWidth;
      }
    });
  });

  return widthMap;
}

const ExtractedMetadataTable = ({
  extractedMetadata,
}: {
  extractedMetadata: IExtractedMetadata[];
}) => {
  if (!extractedMetadata.length) return null;
  const widthMap = createWidthMapFromObjects(
    extractedMetadata.map((em) => em.metadata)
  );
  const columns: GridColDef[] = Object.keys(extractedMetadata[0].metadata).map(
    (key) => {
      return {
        field: key,
        headerName: key,
        width: widthMap[key],
        renderCell: (params) => {
          return (
            <Box sx={{ overflowX: "scroll" }}>
              <Stack gap={1} direction="row">
                {typeof params.value === "string"
                  ? params.value
                  : JSON.stringify(params.value)}
              </Stack>
            </Box>
          );
        },
      };
    }
  );

  return (
    <Box
      sx={{
        width: "100%",
      }}
    >
      <Typography variant="h4" pb={2}>
        Metadata - {extractedMetadata[0].extractor_name}:
      </Typography>
      <DataGrid
        sx={{ backgroundColor: "white" }}
        autoHeight
        getRowId={getRowId}
        rows={extractedMetadata.map((em) => em.metadata)}
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
