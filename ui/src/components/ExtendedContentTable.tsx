import React, { useMemo, useState } from "react";
import {
  Box,
  TextField,
  Select,
  MenuItem,
  Typography,
  IconButton,
  Tooltip,
  styled
} from "@mui/material";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import { DataGrid, GridColDef } from "@mui/x-data-grid";
import InfoIcon from "@mui/icons-material/Info";
import { ExtractionGraph, IContentMetadata } from "getindexify";
import CopyText from "./CopyText";
import { Link } from "react-router-dom";

const filterContentByGraphName = (contentList: IContentMetadata[], graphName: string): IContentMetadata[] => {
  return contentList.filter(content => content.extraction_graph_names.includes(graphName));
};

const StyledToggleButtonGroup = styled(ToggleButtonGroup)(({ theme }) => ({
  backgroundColor: "#F7F9FC",
  borderRadius: 30,
  "& .MuiToggleButtonGroup-grouped": {
    margin: 4,
    border: "1px #E9EDF1",
    "&.Mui-disabled": {
      border: 0
    },
    "&:not(:first-of-type)": {
      borderRadius: 30
    },
    "&:first-of-type": {
      borderRadius: 30
    }
  }
}));

const StyledToggleButton = styled(ToggleButton)(({ theme }) => ({
  padding: "6px 16px",
  fontSize: "0.875rem",
  fontWeight: 500,
  textTransform: "none",
  "&.Mui-selected": {
    backgroundColor: "#FFFFFF",
    color: "#3296FE",
    "&:hover": {
      backgroundColor: "#E9EDF1"
    }
  }
}));

interface ExtendedContentTableProps {
  content: IContentMetadata[];
  extractionGraph: ExtractionGraph;
  graphName: string;
  namespace: string;
}

const ExtendedContentTable: React.FC<ExtendedContentTableProps> = ({ content, extractionGraph, graphName, namespace }) => {

  const columns: GridColDef[] = [
  {
    field: "id",
    headerName: "ID",
    flex: 1,
    renderCell: params =>
      <Box sx={{ display: "flex", alignItems: "center" }}>
        <Link to={`/${namespace}/content/${params.value}`}>
          {params.value}
        </Link>
        <Tooltip title="Copy">
          <CopyText text={params.value}/>
        </Tooltip>
      </Box>
  },
  { field: "children", headerName: "Children", width: 130, type: "number" },
  { field: "source", headerName: "Source", flex: 1 },
  { field: "parentId", headerName: "Parent ID", flex: 1 },
  { field: "labels", headerName: "Labels", flex: 1 },
  { field: "createdAt", headerName: "Created At", width: 200 }
];


  const [tabValue, setTabValue] = useState<string>("search");
  const [policy, setPolicy] = useState("any");
  const [contentId, setContentId] = useState("");

  const handleTabChange = (
    event: React.MouseEvent<HTMLElement>,
    newTab: string | null
  ) => {
    if (newTab !== null) {
      setTabValue(newTab);
    }
  };

  const filteredContent = useMemo(() => {
    return Array.isArray(content) ? filterContentByGraphName(content, graphName) : [];
  }, [content, graphName]);

  const rows = useMemo(() => {
    return filteredContent.map(item => ({
      id: item.id,
      children: 0,
      labels: JSON.stringify(item.labels),
      parentId: item.parent_id,
      source: item.source,
      createdAt: new Date(item.created_at).toLocaleString()
    }));
  }, [filteredContent]);

  const filteredRows = useMemo(() => {
   if (tabValue === "search") {
    return rows.filter(row => 
      (contentId ? row.id.includes(contentId) : true) &&
      (policy !== "any" ? row.source === policy : true)
    );
  }
   return rows;
  }, [rows, tabValue, contentId , policy]);

  const extractionPolicyOptions = useMemo(() => {
    const extractionPolicies = extractionGraph?.extraction_policies || [];
    return extractionPolicies?.map((policy, index) => ({
      value: policy.name,
      label: policy.name
    })) || [];
  }, [extractionGraph]);

  return (
    <Box
      sx={{
        width: "100%",
        pl: 2,
        pr: 2,
        pb: 2,
        pt: 2,
        backgroundColor: "white",
        borderRadius: "0.5rem"
      }}
    >
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between"
        }}
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6" component="h1" sx={{ mr: 1 }}>
              Content
            </Typography>
            <Tooltip title="Info about content">
              <IconButton size="small">
                <InfoIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <StyledToggleButtonGroup
            value={tabValue}
            exclusive
            onChange={handleTabChange}
            aria-label="content type"
          >
            <StyledToggleButton value="search" aria-label="search">
              Search
            </StyledToggleButton>
            <StyledToggleButton value="ingested" aria-label="ingested">
              Ingested
            </StyledToggleButton>
          </StyledToggleButtonGroup>
        </Box>
        {tabValue === "search" && (
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <TextField
              placeholder="Content Id..."
              variant="outlined"
              size="small"
              value={contentId}
              onChange={e => setContentId(e.target.value)}
              sx={{ mr: 2 }}
            />
            <Select
              value={policy}
              displayEmpty
              size="small"
              onChange={e => setPolicy(e.target.value as string)}
              sx={{ minWidth: 120 }}
            >
              <MenuItem value="" disabled>
                Choose policy
              </MenuItem>
              <MenuItem value="any">
                Any
              </MenuItem>
              {extractionPolicyOptions.map((policy, index) => (
                <MenuItem key={index} value={policy.value}>
                  {policy.label}
                </MenuItem>
              )) || null}
            </Select>
          </Box>
        )}
      </Box>

      <DataGrid
        rows={filteredRows}
        columns={columns}
        initialState={{
          pagination: {
            paginationModel: { page: 0, pageSize: 5 }
          }
        }}
        pageSizeOptions={[5, 10, 20]}
        sx={{
          "& .MuiDataGrid-cell:focus": {
            outline: "none"
          },
          boxShadow: "0px 0px 1px 0px rgba(51, 132, 254, 0.2) inset",
          mt: 2
        }}
      />
    </Box>
  );
};

export default ExtendedContentTable;
