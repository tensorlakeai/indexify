import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IContentMetadata } from "../lib/Indexify/types";
import { Alert, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import ArticleIcon from "@mui/icons-material/Article";
import React from "react";
import moment from "moment";
import { Link } from "react-router-dom";

const ContentTable = ({ content }: { content: IContentMetadata[] }) => {
  const columns: GridColDef[] = [
    {
      field: "id",
      headerName: "ID",
      width: 170,
      renderCell: (params) => (
        <Link to={`/${params.row.namespace}/content/${params.value}`}>
          {params.value}
        </Link>
      ),
    },
    {
      field: "name",
      headerName: "Name",
      width: 200,
    },
    {
      field: "parent_id",
      headerName: "Parent ID",
      width: 200,
    },
    {
      field: "content_type",
      headerName: "ContentType",
      width: 150,
    },
    {
      field: "source",
      headerName: "Source",
      width: 140,
    },
    {
      field: "labels",
      headerName: "Labels",
      width: 300,
      valueGetter: (params) => {
        return JSON.stringify(params.value);
      },
    },
    {
      field: "storage_url",
      headerName: "Storage URL",
      width: 200,
    },
    {
      field: "created_at",
      headerName: "Created At",
      width: 200,
      valueGetter: (params) => {
        return moment(params.value * 1000).format("MM/DD/YYYY h:mm A");
      },
    },
  ];

  const renderContent = () => {
    if (content.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Content Found
          </Alert>
        </Box>
      );
    }
    return (
      <Box sx={{ width: "100%" }}>
        <DataGrid
          sx={{ backgroundColor: "white" }}
          autoHeight
          rows={content}
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

  return (
    <>
      <Stack
        display={"flex"}
        direction={"row"}
        alignItems={"center"}
        spacing={2}
      >
        <ArticleIcon />
        <Typography variant="h3">Content</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ContentTable;
