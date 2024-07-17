import React, { useState } from 'react';
import {
  Stack,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TablePagination,
  Drawer,
  Box,
  IconButton,
  TextField,
  Button
} from '@mui/material';
import { Cpu, CloseCircle } from 'iconsax-react';
import { ExtractionGraph, IndexifyClient, IContentMetadata } from "getindexify";
import { useLoaderData } from 'react-router-dom';
import CopyText from '../../components/CopyText';

interface ContentTableProps {
  loadData: (params: { parentId?: string; startId?: string; pageSize: number; source: string }) => Promise<IContentMetadata[]>;
  extractionPolicy: string;
  onContentClick: (content: IContentMetadata) => void;
}

const ContentTable: React.FC<ContentTableProps> = ({ loadData, extractionPolicy, onContentClick }) => {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(5);
  const [content, setContent] = useState<IContentMetadata[]>([]);
  const [isLastPage, setIsLastPage] = useState(false);
  const [pageHistory, setPageHistory] = useState<{id: string, content: IContentMetadata[]}[]>([]);

  const loadContentList = async (startId?: string) => {
    const result = await loadData({
      startId,
      pageSize: rowsPerPage + 1,
      source: extractionPolicy
    });

    if (result.length <= rowsPerPage) {
      setIsLastPage(true);
    } else {
      result.pop();
      setIsLastPage(false);
    }

    setContent(result);
    return result;
  };

  React.useEffect(() => {
    loadContentList();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rowsPerPage, extractionPolicy]);

  const handleChangePage = async (event: unknown, newPage: number) => {
    if (newPage > page) {
      if (!isLastPage) {
        const lastId = content[content.length - 1]?.id;
        if (lastId) {
          setPageHistory(prev => [...prev, {id: lastId, content}]);
          const newContent = await loadContentList(lastId);
          setContent(newContent);
        }
      }
    } else if (newPage < page && pageHistory.length > 0) {
      const newPageHistory = [...pageHistory];
      const prevPage = newPageHistory.pop();
      if (prevPage) {
        setPageHistory(newPageHistory);
        setContent(prevPage.content);
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

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>ID</TableCell>
            <TableCell>Mime Type</TableCell>
            <TableCell>Labels</TableCell>
            <TableCell>Created At</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {content.map((row) => (
            <TableRow key={row.id}>
              <TableCell>
                <Box sx={{ display: "flex", alignItems: "center" }}>
                  <Typography 
                    onClick={() => onContentClick(row)} 
                    sx={{ cursor: 'pointer', '&:hover': { textDecoration: 'underline' } }}
                  >
                    {row.id}
                  </Typography>
                  <CopyText text={row.id}/>
                </Box>
              </TableCell>
              <TableCell>{row.mime_type}</TableCell>
              <TableCell>
                {typeof row.labels === 'object' && row.labels !== null
                  ? Object.entries(row.labels)
                      .map(([key, value]) => `${key}: ${value}`)
                      .join(', ')
                  : String(row.labels)}
              </TableCell>
              <TableCell>
                {row.created_at ? new Date(row.created_at * 1000).toLocaleString() : ''}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={-1}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
        labelDisplayedRows={() => ''}
        slotProps={{
          actions: {
            nextButton: {
              disabled: isLastPage
            }, 
            previousButton: {
              disabled: page === 0
            }
          }
        }}
      />
    </TableContainer>
  );
};

const ExtractionPoliciesContentPage = () => {
  const { 
    client,
    extractionGraph,
    extractionGraphs,
    extractorName,
    namespace
   } = useLoaderData() as {
      client: IndexifyClient
      extractionGraph: ExtractionGraph
      extractionGraphs: ExtractionGraph[]
      extractorName: string, 
      namespace: string
    }
  const [selectedContent, setSelectedContent] = useState<IContentMetadata | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [searchContentId, setSearchContentId] = useState("");
  const [searchResult, setSearchResult] = useState<IContentMetadata | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const contentLoader = async ({
    parentId,
    startId,
    pageSize,
    source,
  }: {
    parentId?: string;
    startId?: string;
    pageSize: number;
    source: string;
  }): Promise<IContentMetadata[]> => {
    const { contentList } = await client.listContent(extractorName, namespace, {
      namespace,
      extractionGraph: extractorName,
      source,
      parentId,
      startId,
      limit: pageSize,
    });

    return contentList;
  };

  const handleContentClick = (content: IContentMetadata) => {
    setSelectedContent(content);
    setDrawerOpen(true);
  };

  const closeDrawer = () => {
    setDrawerOpen(false);
  };

  const handleSearch = async () => {
    if (!searchContentId) return;
    setIsLoading(true);
    try {
      const result = await client.getContentMetadata(searchContentId);
      setSearchResult(result);
      setSelectedContent(result);
      setDrawerOpen(true);
    } catch (error) {
      console.error("Error searching content:", error);
      setSearchResult(null);
    } finally {
      setIsLoading(false);
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
      <Stack
        display={'flex'}
        direction={'row'}
        alignItems={'center'}
        spacing={1}
        marginBottom={4}
      >
        <div className="heading-icon-container">
          <Cpu size="25" className="heading-icons" variant="Outline" />
        </div>
        <Typography variant="h4">
          Extraction Policies Content
        </Typography>
      </Stack>

      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
        <TextField
          placeholder="Search by Content Id"
          variant="outlined"
          size="small"
          value={searchContentId}
          onChange={(e) => setSearchContentId(e.target.value)}
          sx={{ flexGrow: 1 }}
        />
        <Button variant="contained" onClick={handleSearch} disabled={isLoading}>
          Search
        </Button>
      </Box>

      {extractionGraph.extraction_policies.map((policy) => (
        <Box key={policy.name} sx={{ marginBottom: 4 }}>
          <Typography variant="h6" sx={{ marginBottom: 2 }}>
            {policy.name}
          </Typography>
          <ContentTable 
            loadData={contentLoader} 
            extractionPolicy={policy.name} 
            onContentClick={handleContentClick}
          />
        </Box>
      ))}

      <Drawer
        anchor="right"
        open={drawerOpen}
        onClose={closeDrawer}
      >
        <Box sx={{ width: 400, padding: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">{selectedContent?.id}</Typography>
            <IconButton onClick={closeDrawer}>
              <CloseCircle size="20" />
            </IconButton>
          </Stack>
          {selectedContent && (
            <>
              <Typography variant="body2">Mime Type: {selectedContent.mime_type}</Typography>
              <Typography variant="body2">Created at: {new Date(selectedContent.created_at * 1000).toLocaleString()}</Typography>
              <Typography variant="body2">Labels: {JSON.stringify(selectedContent.labels)}</Typography>
              <Typography variant="body2">Source: {selectedContent.source}</Typography>
              <Typography variant="body2">Parent ID: {selectedContent.parent_id}</Typography>
            </>
          )}
        </Box>
      </Drawer>
    </Box>
  );
};

export default ExtractionPoliciesContentPage;