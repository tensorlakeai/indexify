import React, { useEffect, useState } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Collapse,
  IconButton,
  TextField,
  Button,
  Alert
} from '@mui/material';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import {
  IContentMetadata,
  IndexifyClient,
} from 'getindexify';
import CopyText from '../../components/CopyText';
import ContentAccordion from '../../components/ContentAccordion';

interface PolicyContentTableProps {
  client: IndexifyClient;
  namespace: string;
  contentId: string;
  extractorName: string;
  policyName: string;
}

const Row = (props: { row: IContentMetadata, client: IndexifyClient, namespace: string }) => {
  const { row, client, namespace } = props;
  const [open, setOpen] = useState(false);

  return (
    <React.Fragment>
      <TableRow sx={{ '& > *': { borderBottom: 'unset' } }}>
        <TableCell>
          <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell component="th" scope="row">
          <Box sx={{ display: "flex", alignItems: "center" }}>
            <Typography noWrap sx={{ mr: 1, maxWidth: 'calc(100% - 24px)' }}>{row.id}</Typography>
            <CopyText text={row.id} />
          </Box>
        </TableCell>
        <TableCell>{row.mime_type}</TableCell>
        <TableCell>{row.source || "text_to_chunks"}</TableCell>
        <TableCell>{row.parent_id || '\u00A0'}</TableCell>
        <TableCell>
          {typeof row.labels === 'object' && row.labels !== null
            ? Object.entries(row.labels)
                .map(([key, value]) => `${key}: ${value}`)
                .join(', ')
            : row.labels || '\u00A0'}
        </TableCell>
        <TableCell>
          {row.created_at ? new Date(row.created_at * 1000).toLocaleString() : '\u00A0'}
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={7}>
          <Collapse in={open} timeout="auto" unmountOnExit>
            <Box sx={{ margin: 1 }}>
              <ContentAccordion
                content={row}
                client={client}
                namespace={namespace}
              />
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
};

const PolicyContentTable: React.FC<PolicyContentTableProps> = ({ 
  client, 
  namespace, 
  contentId, 
  extractorName, 
  policyName
}) => {
  const [content, setContent] = useState<IContentMetadata[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [searchContentId, setSearchContentId] = useState("");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const loadContent = async () => {
    setIsLoading(true);
    try {
      const result = await client.getExtractionPolicyContent({
        contentId: searchContentId ? searchContentId : contentId,
        graphName: extractorName,
        policyName: policyName
      });
      
      setContent(result);
    } catch (error) {
      console.error(`Error loading content for policy ${policyName}:`, error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadContent();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, searchContentId, extractorName, namespace, policyName]);

  const handleSearch = () => {
    loadContent();
  };

  const handleOnEnter = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      handleSearch();
    }
  };

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
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
    <>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
        <TextField
          placeholder="Search by Content Id"
          variant="outlined"
          size="small"
          value={searchContentId}
          onChange={(e) => setSearchContentId(e.target.value)}
          onKeyDown={handleOnEnter}
          sx={{ flexGrow: 1 }}
        />
        <Button variant="contained" onClick={handleSearch} disabled={isLoading}>
          Search
        </Button>
      </Box>
      <TableContainer component={Paper} sx={{boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset",}}>
        <Table aria-label="collapsible table">
          <TableHead>
            <TableRow>
              <TableCell />
              <TableCell>Content ID</TableCell>
              <TableCell>Mime Type</TableCell>
              <TableCell>Source</TableCell>
              <TableCell>Parent ID</TableCell>
              <TableCell>Labels</TableCell>
              <TableCell>Created At</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {content
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((row) => (
                <Row key={row.id} row={row} client={client} namespace={namespace} />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={content.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </>
  );
};

export default PolicyContentTable;
