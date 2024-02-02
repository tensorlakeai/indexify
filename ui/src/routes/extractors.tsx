import { useEffect, useState } from "react";
import IndexifyClient from "../lib/Indexify/client";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import { Typography } from "@mui/material";
import Extractor from "../lib/Indexify/extractor";

const ExtractorsPage = () => {
  const client = new IndexifyClient();

  const [extractors, setExtractors] = useState<Extractor[]>([]);
  useEffect(() => {
    client.extractors().then((extractors) => {
      setExtractors(extractors);
    });
  }, []);

  const columns: GridColDef[] = [
    { field: "name", headerName: "Name", width: 300 },
    { field: "description", headerName: "Description", width: 300 },
    {
      field: "input_params",
      headerName: "Input Parameters",
      width: 300,
      valueGetter: (params) => {
        console.log(params);
        return JSON.stringify(params.value);
      },
    },
    {
      field: "outputs",
      headerName: "Outputs",
      width: 300,
      valueGetter: (params) => {
        console.log(params);
        return JSON.stringify(params.row.input_params);
      },
    },
  ];

  const getRowId = (row: Extractor) => {
    return row.name;
  };

  return (
    <div>
      <Typography mb={3} variant="h3" component="h1">
        Extractors
      </Typography>
      <div style={{ height: 400, width: "100%" }}>
        <DataGrid
          getRowId={getRowId}
          rows={extractors}
          columns={columns}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 5 },
            },
          }}
          pageSizeOptions={[5, 10]}
        />
      </div>
    </div>
  );
};

export default ExtractorsPage;
