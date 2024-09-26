import React, { useEffect, useMemo, useState } from "react";
import {
  Box,
  TextField,
  Typography,
  IconButton,
  Tooltip,
  styled,
  Table as MuiTable,
  TableBody as MuiTableBody,
  TableCell as MuiTableCell,
  TableContainer as MuiTableContainer,
  TableHead as MuiTableHead,
  TableRow as MuiTableRow,
  Button,
  Paper,
  TablePagination,
  Alert
} from "@mui/material";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import InfoIcon from "@mui/icons-material/Info";
import DeleteIcon from '@mui/icons-material/Delete';
import { ExtractionGraph, IContentMetadata, IndexifyClient } from "getindexify";
import CopyText from "./CopyText";
import { Link } from "react-router-dom";
import UploadButton from "./UploadButton";

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
  const [contentId, setContentId] = useState("");
  const [contentList, setContentList] = useState<ContentList | undefined>(undefined);
  const [searchResult, setSearchResult] = useState<IContentMetadata[] | null>(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(5);
  const [isLoading, setIsLoading] = useState(false);
  const [pageHistory, setPageHistory] = useState<{id: string, content: IContentMetadata[]}[]>([]);
  const [isLastPage, setIsLastPage] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);

  const loadContentList = async (startId?: string) => {
    setIsLoading(true);
    try {
      const result = await client.listContent(extractionGraph.name, undefined, {
        namespace: namespace,
        extractionGraph: extractionGraph.name,
        limit: rowsPerPage + 1,
        startId: startId,
        source: tabValue === "ingested" ? "ingestion" : undefined
      });
      
      if (result.contentList.length <= rowsPerPage) {
        setIsLastPage(true);
      } else {
        result.contentList.pop();
        setIsLastPage(false);
      }
      
      setContentList(result);
      return result.contentList;
    } catch (error) {
      console.error("Error loading content:", error);
      return [];
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (tabValue === "ingested") {
      loadContentList();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, extractionGraph.name, namespace, rowsPerPage, tabValue]);

  const handleSearch = async () => {
    if (!contentId) return;
    setIsLoading(true);
    setHasSearched(true);
    try {
      const result = await client.getContentMetadata(contentId);
      setSearchResult(result ? [result] : []);
    } catch (error) {
      console.error("Error searching content:", error);
      setSearchResult([]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleOnEnter = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Enter' && !isLoading && contentId) {
      handleSearch();
    }
  };

  const filteredContent = useMemo(() => {
    if (tabValue === "search") {
      return searchResult || [];
    }
    if (!contentList) return [];
    return filterContentByGraphName(contentList.contentList, graphName);
  }, [contentList, graphName, tabValue, searchResult]);

  const handleChangePage = async (event: unknown, newPage: number) => {
    if (tabValue === "search") return;
    if (newPage > page) {
      if (!isLastPage) {
        const lastId = contentList?.contentList[contentList.contentList.length - 1]?.id;
        if (lastId) {
          setPageHistory(prev => [...prev, {id: lastId, content: contentList.contentList}]);
          const newContent = await loadContentList(lastId);
          setContentList(prev => ({ ...prev, contentList: newContent }));
        }
      }
    } else if (newPage < page && pageHistory.length > 0) {
      const newPageHistory = [...pageHistory];
      const prevPage = newPageHistory.pop();
      if (prevPage) {
        setPageHistory(newPageHistory);
        setContentList(prev => ({ ...prev, contentList: prevPage.content }));
        setIsLastPage(false);
      }
    }
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
    setPageHistory([]);
    loadContentList();
  };

  const handleTabChange = (event: React.MouseEvent<HTMLElement>, newTab: string | null) => {
    if (newTab !== null) {
      setTabValue(newTab);
      setPage(0);
      setPageHistory([]);
      setSearchResult(null);
      setHasSearched(false);
      if (newTab === "ingested") {
        loadContentList();
      }
    }
  };

  const handleDelete = async (contentId: string) => {
    try {
      await client.deleteContent(namespace, contentId);
      if (tabValue === "ingested") {
        loadContentList();
      } else {
        setSearchResult((prev) => prev ? prev.filter(item => item.id !== contentId) : null);
      }
    } catch (error) {
      console.error("Error deleting content:", error);
    }
  };

  return (
    <Box sx={{
        width: "100%",
        pl: 2,
        pr: 2,
        pb: 2,
        pt: 2,
        backgroundColor: "white",
        borderRadius: "0.5rem",
        boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",
      }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
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
          <StyledToggleButton value="ingested" aria-label="search">
            Ingested
          </StyledToggleButton>
        </StyledToggleButtonGroup>
        </Box>
        <UploadButton client={client} extractionGraph={extractionGraph.name} />
      </Box>

      {tabValue === "search" && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
          <TextField
            placeholder="Search by Content Id"
            variant="outlined"
            size="small"
            value={contentId}
            onChange={(e) => setContentId(e.target.value)}
            onKeyDown={handleOnEnter}
            sx={{ flexGrow: 1 }}
          />
          <Button variant="contained" onClick={handleSearch} disabled={isLoading || !contentId}>
            Search
          </Button>
        </Box>
      )}
      <MuiTableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset"}}>
        {filteredContent.length > 0 ? (
          <MuiTable>
            {(tabValue !== "search" || (tabValue === "search" && hasSearched)) && (
              <MuiTableHead>
                <MuiTableRow>
                  <MuiTableCell>Content ID</MuiTableCell>
                  <MuiTableCell>Mime Type</MuiTableCell>
                  {tabValue !== "ingested" && (
                    <MuiTableCell>Source</MuiTableCell>
                  )}
                  {tabValue !== "ingested" && (
                    <MuiTableCell>Parent ID</MuiTableCell>
                  )}
                  <MuiTableCell>Labels</MuiTableCell>
                  <MuiTableCell>Created At</MuiTableCell>
                  <MuiTableCell>Actions</MuiTableCell>
                </MuiTableRow>
              </MuiTableHead>
            )}
            <MuiTableBody>
              {filteredContent.map((row) => (
                <MuiTableRow key={row.id}>
                  <MuiTableCell>
                    <Box sx={{ display: "flex", alignItems: "center" }}>
                      <Link to={`/${namespace}/extraction-graphs/${graphName}/content/${row.id}`}>
                        {row.id}
                      </Link>
                      <CopyText text={row.id}/>
                    </Box>
                  </MuiTableCell>
                  <MuiTableCell>{row.mime_type}</MuiTableCell>
                  {tabValue !== "ingested" && (
                    <MuiTableCell>{row.source ? row.source : "Ingestion"}</MuiTableCell>
                  )}
                  {tabValue !== "ingested" && (
                    <MuiTableCell>{row.parent_id}</MuiTableCell>
                  )}
                  <MuiTableCell>
                    {typeof row.labels === 'object' && row.labels !== null
                      ? Object.entries(row.labels)
                          .map(([key, value]) => `${key}: ${value}`)
                          .join(', ')
                      : String(row.labels)}
                  </MuiTableCell>
                  <MuiTableCell>
                    {row.created_at ? new Date(row.created_at * 1000).toLocaleString() : ''}
                  </MuiTableCell>
                  <MuiTableCell>
                    <IconButton onClick={() => handleDelete(row.id)} aria-label="delete">
                      <DeleteIcon color="error" />
                    </IconButton>
                  </MuiTableCell>
                </MuiTableRow>
              ))}
            </MuiTableBody>
          </MuiTable>
        ) : (
          (tabValue === "search" && hasSearched) || (tabValue === "ingested" && !isLoading) ? (
            <Alert variant="standard" severity="info">
              No Content Found
            </Alert>
          ) : null
        )}
      </MuiTableContainer>
      {filteredContent.length > 0 && tabValue === "ingested" && (
        <TablePagination
          rowsPerPageOptions={[5, 10, 20]}
          component="div"
          count={-1}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          labelDisplayedRows={() => ''}
          slotProps={
            {
              actions: 
              {
                nextButton: {
                  disabled: isLastPage
                }, 
                previousButton: {
                  disabled: page === 0
                }
              }
            }
          }
          sx={{ display: "flex", ".MuiTablePagination-toolbar": {
            paddingLeft: "10px"
          }, ".MuiTablePagination-actions": {
            marginLeft: "0px !important"
          }}}
        />
      )}
    </Box>
  );
};

export default ExtendedContentTable;
