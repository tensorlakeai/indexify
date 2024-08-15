import React, { useEffect, useState } from 'react';
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableContainer, 
  TableHead, 
  TableRow, 
  Paper, 
  TablePagination,
  Alert, 
  Chip, 
  Box,
  Link
} from '@mui/material';
import { IExtractionPolicy, IndexifyClient, ITask, TaskStatus } from 'getindexify';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import { formatTimestamp } from '../utils/helpers';

interface TasksTableProps {
  extractionPolicies: IExtractionPolicy[];
  namespace: string;
  loadData: (pageSize: number, startId?: string) => Promise<{ tasks: ITask[]; total: number; hasNextPage: boolean }>;
  hideContentId?: boolean;
  hideExtractionPolicy?: boolean;
  onContentClick: (contentId: string) => void;
  client: IndexifyClient;
}

const TasksTable: React.FC<TasksTableProps> = ({
  hideContentId,
  hideExtractionPolicy,
  loadData,
  onContentClick,
}) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [tasks, setTasks] = useState<ITask[]>([]);
  const [page, setPage] = useState<number>(0);
  const [rowsPerPage, setRowsPerPage] = useState<number>(20);
  const [isLastPage, setIsLastPage] = useState<boolean>(false);

  const loadTasks = async () => {
    setLoading(true);
    try {
      const startId = tasks.length > 0 ? tasks[tasks.length - 1].id : undefined;
      const { tasks: newTasks } = await loadData(rowsPerPage + 1, startId);
      
      if (newTasks.length <= rowsPerPage) {
        setIsLastPage(true);
      } else {
        newTasks.pop();
        setIsLastPage(false);
      }
      
      setTasks(newTasks);
    } catch (error) {
      console.error("Error loading tasks:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTasks();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, rowsPerPage]);

  const handleChangePage = (event: React.MouseEvent<HTMLButtonElement> | null, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const renderStatusChip = (status: TaskStatus) => {
    switch (status) {
      case TaskStatus.Failure:
        return <Chip icon={<ErrorOutlineIcon />} label="Failure" variant="outlined" color="error" sx={{ backgroundColor: '#FBE5E5' }} />;
      case TaskStatus.Success:
        return <Chip icon={<CheckCircleOutlineIcon />} label="Success" variant="outlined" color="success" sx={{ backgroundColor: '#E5FBE6' }} />;
      default:
        return <Chip icon={<InfoOutlinedIcon />} label="In Progress" variant="outlined" color="info" sx={{ backgroundColor: '#E5EFFB' }} />;
    }
  };

  const renderContent = () => {
    if (tasks.length === 0 && !loading) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="standard" severity="info">
            No Tasks Found
          </Alert>
        </Box>
      );
    }
    return (
      <TableContainer component={Paper} sx={{ backgroundColor: 'white', borderRadius: '12px', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset" }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Task ID</TableCell>
              {!hideContentId && <TableCell>Content ID</TableCell>}
              {!hideExtractionPolicy && <TableCell>Extraction Policy</TableCell>}
              <TableCell>Status</TableCell>
              <TableCell>Source</TableCell>
              <TableCell>Created At</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tasks?.map((task) => (
              <TableRow key={task.id}>
                <TableCell>{task.id}</TableCell>
                {!hideContentId && (
                  <TableCell>
                    <Link
                      component="button"
                      variant="body2"
                      onClick={() => onContentClick(task.content_metadata.id)}
                    >
                      {task.content_metadata.id}
                    </Link>
                  </TableCell>
                )}
                {!hideExtractionPolicy && (
                  <TableCell>
                    {task.extraction_policy_id}
                  </TableCell>
                )}
                <TableCell>{renderStatusChip(task.outcome as TaskStatus)}</TableCell>
                <TableCell>{task.content_metadata.source || 'Ingestion'}</TableCell>
                <TableCell>
                  {formatTimestamp(task.content_metadata.created_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
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
      </TableContainer>
    );
  };

  return <>{renderContent()}</>;
};

export default TasksTable;
