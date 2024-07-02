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

const rows = [
  {
    id: "00b2a11759dce114",
    children: 0,
    labels: '{width"284","',
    createdAt: "12/06/2024 12:05 AM"
  },
  {
    id: "00b2a11759dce114",
    children: 0,
    labels: '{width"284","',
    createdAt: "12/06/2024 12:05 AM"
  },
  {
    id: "00b2a11759dce114",
    children: 0,
    labels: '{width"284","',
    createdAt: "12/06/2024 12:05 AM"
  },
  {
    id: "00b2a11759dce114",
    children: 0,
    labels: '{width"284","',
    createdAt: "12/06/2024 12:05 AM"
  },
  {
    id: "00b2a11759dce114",
    children: 0,
    labels: '{width"284","',
    createdAt: "12/06/2024 12:05 AM"
  }
];

const ExtendedContentTable: React.FC = () => {
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
            <MenuItem value="policy1">Policy 1</MenuItem>
            <MenuItem value="policy2">Policy 2</MenuItem>
            <MenuItem value="policy3">Policy 3</MenuItem>
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
          boxShadow: "0px 0px 1px 0px rgba(51, 132, 252, 0.2) inset",
          mt: 2
        }}
      />
    </Box>
  );
};

export default ExtendedContentTable;
