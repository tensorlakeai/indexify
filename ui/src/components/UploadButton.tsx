import { useState } from 'react';
import UploadIcon from '@mui/icons-material/Upload';
import {
  Box,
  Button,
  Modal,
  Typography,
  MenuItem,
  Select,
  Paper,
  CircularProgress,
} from '@mui/material';
import { ExtractionGraph, IndexifyClient } from 'getindexify';
import LabelsInput from './Inputs/LabelsInput';
import FileDropZone from './Inputs/DropZone';

interface Props {
  client: IndexifyClient;
  extractionGraphs: ExtractionGraph[]
}

const uploadFiles = async (client: IndexifyClient, extractionGraphName: string, files: File[], labels: Record<string, string>): Promise<void> => {
    const uploadPromises = files.map(file => client.uploadFile(extractionGraphName, file, labels));
    await Promise.all(uploadPromises);
  };

const UploadButton = ({ client, extractionGraphs }: Props) => {
  const [open, setOpen] = useState(false);
  const [files, setFiles] = useState<File[]>([]);
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [extractionGraphName, setExtractionGraphName] = useState('');
  const [loading, setLoading] = useState(false);
  const [localExtractionGraphs, setLocalExtractionGraphs] = useState<ExtractionGraph[]>(
    extractionGraphs
  );

  const handleFileSelect = (selectedFiles: File[]) => {
    setFiles(selectedFiles);
  };

  const handleOpen = () => setOpen(true);
  const handleClose = () => setOpen(false);

  const modalStyle = {
    position: 'absolute' as 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: '80%',
    maxWidth: '800px',
    bgcolor: 'background.paper',
    maxHeight: '100%',
    overflow: 'scroll',
    boxShadow: 24,
    p: 4,
    borderRadius: "12px",
  };

  const upload = async () => {
    if (files.length > 0 && extractionGraphName) {
      setLoading(true);
      await uploadFiles(client, extractionGraphName, files, labels);
      window.location.reload();
    }
  };

  const updateExtractionGraphs = async () => {
    const graphs = await client.getExtractionGraphs();
    setLocalExtractionGraphs(graphs);
  };

  const isUploadButtonDisabled = files.length === 0 || !extractionGraphName || loading;

  return (
    <>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="modal-modal-title"
        aria-describedby="modal-modal-description"
      >
        <Paper sx={modalStyle}>
          <Typography id="modal-modal-title" variant="h6" component="h2" sx={{ fontWeight: 500 }}>
            Upload Content
          </Typography>
          <Typography id="modal-modal-description" sx={{ mt: 2, fontWeight: 500, color: "#4A4F56" }}>
            Select a file to upload and choose an extraction graph.
          </Typography>
          <Select
            disabled={loading}
            onFocus={updateExtractionGraphs}
            value={extractionGraphName}
            onChange={(e) => setExtractionGraphName(e.target.value)}
            displayEmpty
            fullWidth
            variant="outlined"
            sx={{ mt: 2, color: '#757A82' }}
            size="small"
          >
            <MenuItem value="" disabled>
              Select Extraction Graph
            </MenuItem>
            {localExtractionGraphs.map((graph) => (
              <MenuItem key={graph.name} value={graph.name}>
                {graph.name}
              </MenuItem>
            ))}
          </Select>
          <FileDropZone onFileSelect={handleFileSelect} />
          <LabelsInput
            disabled={loading}
            onChange={(val) => {
              setLabels(val);
            }}
          />

          <Box sx={{ mt: 2, position: 'relative' }} gap={2} display={'flex'} justifyContent={'flex-end'}>
            <Button
              variant="outlined"
              onClick={handleClose}
              sx={{ color: '#3296FE', border: 1, borderColor: '#DFE5ED' }}
            >
              Cancel
            </Button>
            <Button
              variant={isUploadButtonDisabled ? 'outlined' : 'contained'}
              onClick={upload}
              disabled={isUploadButtonDisabled}
              sx={{ padding: "12px", ...(isUploadButtonDisabled && {
                  backgroundColor: '#E9EDF1',
                  color: '#757A82'
                }) }}
            >
              Upload Content
            </Button>
            {loading && (
              <CircularProgress
                size={24}
                sx={{
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  marginTop: '-12px',
                  marginLeft: '-12px',
                }}
              />
            )}
          </Box>
        </Paper>
      </Modal>
      <Button onClick={handleOpen} size="small" variant="contained" color="primary" startIcon={<UploadIcon />}>
        Upload
      </Button>
    </>
  );
};

export default UploadButton;
