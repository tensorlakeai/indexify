import React, { useEffect, useMemo, useState } from "react";
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
import { DataGrid, GridColDef, GridPaginationModel } from "@mui/x-data-grid";
import InfoIcon from "@mui/icons-material/Info";
import { ExtractionGraph, IContentMetadata, IndexifyClient } from "getindexify";
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

type ContentList = {
    contentList: IContentMetadata[];
    total?: number;
}

interface ExtendedContentTableProps {
  client: IndexifyClient;
  extractionGraph: ExtractionGraph;
  graphName: string;
  namespace: string;
}

const ExtendedContentTable: React.FC<ExtendedContentTableProps> = ({ client, extractionGraph, graphName, namespace }) => {
  const [tabValue, setTabValue] = useState<string>("ingested");
  const [policy, setPolicy] = useState("any");
  const [contentId, setContentId] = useState("");
  const [contentList, setContentList] = useState<ContentList | undefined>(undefined);
  const [paginationModel, setPaginationModel] = useState<GridPaginationModel>({
    page: 0,
    pageSize: 5
  });
  const [isLoading, setIsLoading] = useState(false);

  const contentStartId = (): string | undefined => {
    const length = contentList?.contentList.length ?? 0;
    const startID = length > 0 ? contentList?.contentList[length - 1]?.id : undefined;
    return startID;
  }

  useEffect(() => {
    const loadContentList = async () => {
      setIsLoading(true);
      try {
        const result = await client.listContent(extractionGraph.name, undefined, {
          namespace: namespace,
          extractionGraph: extractionGraph.name,
          limit: paginationModel.pageSize,
          startId: contentStartId(),
          source: tabValue === "ingested" ? "ingestion" : undefined,
          returnTotal: true
        });
        setContentList(result);
      } catch (error) {
        console.error("Error loading content:", error);
      } finally {
        setIsLoading(false);
      }
    }

    loadContentList();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, extractionGraph.name, namespace, paginationModel, tabValue]);

  const filteredContent = useMemo(() => {
    if (!contentList) return [];
    return filterContentByGraphName(contentList.contentList, graphName).filter(content => 
      (contentId ? content.id.includes(contentId) : true) &&
      (policy !== "any" ? content.source === policy : true)
    );
  }, [contentList, graphName, contentId, policy]);

  const handlePaginationModelChange = (newModel: GridPaginationModel) => {
    setPaginationModel((prev)=> {
      if(prev.pageSize !== newModel.pageSize){
        return {
          page: 0,
          pageSize: newModel.pageSize
        }
      } else {
        return newModel
      }
    });
  };

  const handleTabChange = (event: React.MouseEvent<HTMLElement>, newTab: string | null) => {
    if (newTab !== null) {
      setTabValue(newTab);
      setPaginationModel({ page: 0, pageSize: paginationModel.pageSize });
    }
  };

  const columns: GridColDef[] = [
    {
      field: "id",
      headerName: "ID",
      flex: 1,
      renderCell: params =>
        <Box sx={{ display: "flex", alignItems: "center" }}>
          <Link to={`/${namespace}/extraction-graphs/${graphName}/content/${params.value}`}>
            {params.value}
          </Link>
          <Tooltip title="Copy">
            <CopyText text={params.value}/>
          </Tooltip>
        </Box>
    },
    { field: "mime_type", headerName: "Mime Type", width: 130, type: "number" },
    { field: "source", headerName: "Source", flex: 1 },
    { field: "parent_id", headerName: "Parent ID", flex: 1 },
    { field: "labels", headerName: "Labels", flex: 1, valueGetter: (params) => {
      if (typeof params.value === 'object' && params.value !== null) {
        return Object.entries(params.value)
          .map(([key, value]) => `${key}: ${value}`)
          .join(', ');
      }
      return String(params.value);
    } },
    { field: "created_at", headerName: "Created At", width: 200, valueGetter: (params) => {
      if (params.value) {
        const date = new Date(params.value * 1000);
        return date.toLocaleString();
      }
      return '';
    } }
  ];

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
        borderRadius: "0.5rem",
        boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",
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
              <IconButton size="small" href="https://docs.getindexify.ai/concepts/#content" target="_blank">
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
              ))}
            </Select>
          </Box>
        )}
      </Box>

      <DataGrid
        rows={filteredContent}
        columns={columns}
        paginationModel={paginationModel}
        initialState={{
          pagination: {
            paginationModel: {
              page: 0,
              pageSize: 5
            }
          }
        }}
        pagination
        paginationMode="server"
        onPaginationModelChange={handlePaginationModelChange}
        pageSizeOptions={[5, 10, 20]}
        loading={isLoading}
        sx={{
          "& .MuiDataGrid-cell:focus": {
            outline: "none"
          },
          mt: 2,
          '.MuiTablePagination-displayedRows': {
            display: 'none',
          },
          '.MuiTablePagination-actions': {
            marginLeft: '0px !important'
          },
          '.MuiTablePagination-input': {
            marginRight: '10px !important'
          }
        }}
        rowCount={-1}
      />
    </Box>
  );
};

export default ExtendedContentTable;
