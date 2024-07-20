import React, { useEffect, useState } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  Table as MuiTable, 
  TableContainer as MuiTableContainer, 
  TableHead as MuiTableHead, 
  TableRow as MuiTableRow, 
  TableCell as MuiTableCell, 
  TableBody as MuiTableBody,
  Alert,
  TablePagination
} from '@mui/material'
import {
  IContentMetadata,
  IndexifyClient,
} from 'getindexify'
import CopyText from '../../components/CopyText'

interface PolicyContentTableProps {
  client: IndexifyClient;
  namespace: string;
  contentId: string;
  extractorName: string;
  policyName: string;
  onContentClick: (content: IContentMetadata) => void;
}

const PolicyContentTable: React.FC<PolicyContentTableProps> = ({ 
  client, 
  namespace, 
  contentId, 
  extractorName, 
  policyName,
  onContentClick 
}) => {
  const [content, setContent] = useState<IContentMetadata[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(5);
  const [pageHistory, setPageHistory] = useState<{id: string, content: IContentMetadata[]}[]>([]);
  const [isLastPage, setIsLastPage] = useState(false);

  const loadContent = async (startId?: string) => {
    setIsLoading(true);
    try {
      const result = await client.listContent(extractorName, namespace, {
        namespace: namespace,
        parentId: contentId,
        extractionGraph: extractorName,
        source: policyName,
        limit: rowsPerPage + 1,
        startId: startId,
      });
      
      if (result.contentList.length <= rowsPerPage) {
        setIsLastPage(true);
      } else {
        result.contentList.pop();
        setIsLastPage(false);
      }
      
      setContent(result.contentList);
      return result.contentList;
    } catch (error) {
      console.error(`Error loading content for policy ${policyName}:`, error);
      return [];
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadContent();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, contentId, extractorName, namespace, policyName, rowsPerPage]);

  const handleChangePage = async (event: unknown, newPage: number) => {
    if (newPage > page) {
      if (!isLastPage) {
        const lastId = content[content.length - 1]?.id;
        if (lastId) {
          setPageHistory(prev => [...prev, {id: lastId, content: content}]);
          const newContent = await loadContent(lastId);
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
    loadContent();
  };

  if (isLoading && content.length === 0) {
    return <Typography>Loading...</Typography>;
  }

  if (content.length === 0) {
    return (
      <Box mt={1} mb={2}>
        <Alert variant="standard" severity="info">
          No Content Found
        </Alert>
      </Box>
    );
  }

  return (
    <Box>
      <MuiTableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",}}>
        <MuiTable>
          <MuiTableHead>
            <MuiTableRow>
              <MuiTableCell>Content ID</MuiTableCell>
              <MuiTableCell>Mime Type</MuiTableCell>
              <MuiTableCell>Source</MuiTableCell>
              <MuiTableCell>Parent ID</MuiTableCell>
              <MuiTableCell>Labels</MuiTableCell>
              <MuiTableCell>Created At</MuiTableCell>
            </MuiTableRow>
          </MuiTableHead>
          <MuiTableBody>
            {content.map((row) => (
              <MuiTableRow key={row.id}>
                <MuiTableCell>
                  <Box sx={{ display: "flex", alignItems: "center" }}>
                    <Typography
                      onClick={() => onContentClick(row)}
                      sx={{ cursor: 'pointer', '&:hover': { textDecoration: 'underline' } }}
                    >
                      {row.id}
                    </Typography>
                    <CopyText text={row.id}/>
                  </Box>
                </MuiTableCell>
                <MuiTableCell>{row.mime_type}</MuiTableCell>
                <MuiTableCell>{row.source}</MuiTableCell>
                <MuiTableCell>{row.parent_id}</MuiTableCell>
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
              </MuiTableRow>
            ))}
          </MuiTableBody>
        </MuiTable>
      </MuiTableContainer>
      <TablePagination
        rowsPerPageOptions={[5, 10, 20]}
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
        sx={{ 
          display: "flex", 
          ".MuiTablePagination-toolbar": {
            paddingLeft: "10px"
          }, 
          ".MuiTablePagination-actions": {
            marginLeft: "0px !important"
          }
        }}
      />
    </Box>
  );
};

export default PolicyContentTable;