import React, { useState } from "react";
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
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { ExtractionGraph } from "getindexify";

// Styled components remain the same
// Styled components for the TabSwitcher
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

interface IBaseContentMetadata {
  id: string;
  parent_id: string;
  root_content_id: string;
  namespace: string;
  name: string;
  mime_type: string;
  labels: Record<string, string>;
  storage_url: string;
  created_at: number;
  source: string;
  size: number;
  hash: string;
  extraction_graph_names: string[];
}

interface IContentMetadata extends IBaseContentMetadata {
  content_url: string;
}

interface ExtendedContentTableProps {
  content: IContentMetadata[];
  extractionGraph: ExtractionGraph;
}

const columns: GridColDef[] = [
  {
    field: "id",
    headerName: "ID",
    flex: 1,
    renderCell: params =>
      <Box sx={{ display: "flex", alignItems: "center" }}>
        <Typography color="primary" sx={{ cursor: "pointer" }}>
          {params.value}
        </Typography>
        <Tooltip title="Copy">
          <IconButton size="small">
            <ContentCopyIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>
  },
  { field: "children", headerName: "CHILDREN", width: 130, type: "number" },
  { field: "labels", headerName: "LABELS", flex: 1 },
  { field: "createdAt", headerName: "CREATED AT", width: 200 }
];

const ExtendedContentTable: React.FC<ExtendedContentTableProps> = ({ content, extractionGraph }) => {
  const [tabValue, setTabValue] = useState<string>("ingested");
  const [policy, setPolicy] = useState("");
  const [contentId, setContentId] = useState("");

  const handleTabChange = (
    event: React.MouseEvent<HTMLElement>,
    newTab: string | null
  ) => {
    if (newTab !== null) {
      setTabValue(newTab);
    }
  };

  const rows = content.map(item => ({
    id: item.id,
    children: 0, // You may want to calculate this based on the content structure
    labels: JSON.stringify(item.labels),
    createdAt: new Date(item.created_at).toLocaleString()
  }));

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
            {extractionGraph.extraction_policies.map((policy, index) => (
              <MenuItem key={index} value={`policy${index + 1}`}>
                Policy {index + 1}
              </MenuItem>
            ))}
          </Select>
        </Box>
      </Box>

      <DataGrid
        rows={rows}
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
