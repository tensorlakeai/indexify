import React from "react";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { IExtractionPolicy, ITask } from "getindexify";
import { Alert, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import TaskIcon from '@mui/icons-material/Task';
import moment from "moment";
import { Link } from "react-router-dom";

const TasksTable = ({
  namespace,
  policies,
  tasks,
  hideContentId,
  hideExtractionPolicy,
}: {
  namespace: string;
  policies: IExtractionPolicy[];
  tasks: ITask[];
  hideContentId?: boolean;
  hideExtractionPolicy?: boolean;
}) => {
  let columns: GridColDef[] = [
    {
      field: "id",
      headerName: "Task ID",
      width: 170,
    },
    {
      field: "content_metadata.id",
      headerName: "Content ID",
      width: 170,
      valueGetter: (params) => params.row.content_metadata.id,
      renderCell: (params) => (
        <Link to={`/${namespace}/content/${params.value}`}>{params.value}</Link>
      ),
    },
    {
      field: "extraction_policy_id",
      headerName: "Extraction Policy",
      renderCell: (params) => {
        const policy = policies.find(policy => policy.id === params.value)
        return policy ? <Link to={`/${namespace}/extraction-policies/${policy?.name}`}>
          {policy?.name}
        </Link> : null
      },
      width: 200,
    },
    {
      field: "outcome",
      headerName: "Outcome",
      width: 100,
    },
    {
      field: "content_metadata.storage_url",
      headerName: "Storage URL",
      valueGetter: (params) => params.row.content_metadata.storage_url,
      width: 200,
    },
    {
      field: "content_metadata.source",
      headerName: "Source",
      valueGetter: (params) => params.row.content_metadata.source,
      width: 170,
    },
    {
      field: "content_metadata.created_at",
      headerName: "Created At",
      renderCell: (params) => {
        return moment(params.row.content_metadata.created_at * 1000).format(
          "MM/DD/YYYY h:mm A"
        );
      },
      valueGetter: (params) => params.row.content_metadata.created_at,
      width: 200,
    },
  ];

  columns = columns.filter((col) => {
    if (hideContentId && col.field === "content_metadata.id") {
      return false;
    } else if (hideExtractionPolicy && col.field === "extraction_policy_id") {
      return false;
    }
    return true;
  });

  const renderContent = () => {
    if (tasks.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Tasks Found
          </Alert>
        </Box>
      );
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
        <TaskIcon />
        <Typography variant="h3">Tasks</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default TasksTable;
