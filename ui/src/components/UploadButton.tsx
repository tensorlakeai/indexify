import { useState } from 'react';
import UploadIcon from '@mui/icons-material/Upload';
import {
  Box,
  Button,
  Modal,
  Typography,
  Paper,
  CircularProgress,
  OutlinedInput,
  FormHelperText,
} from '@mui/material';
import { IndexifyClient } from 'getindexify';
import LabelsInput from './Inputs/LabelsInput';
import FileDropZone from './Inputs/DropZone';

interface Props {
  client: IndexifyClient;
  extractionGraph: string;
}

const UploadButton = ({ client, extractionGraph }: Props) => {
  const [open, setOpen] = useState(false);
  const [files, setFiles] = useState<File[]>([]);
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [newContentId, setNewContentId] = useState("");
  const [contentIdError, setContentIdError] = useState("");

  const handleFileSelect = (selectedFiles: File[]) => {
    setFiles(selectedFiles);
  };

  const handleOpen = () => setOpen(true);
  const handleClose = () => setOpen(false);

  const validateContentId = (value: string) => {
    const hexRegex = /^[0-9A-Fa-f]*$/;
    if (value === "" || hexRegex.test(value)) {
      setNewContentId(value);
      setContentIdError("");
    } else {
      setContentIdError("Content ID must be a valid hexadecimal string");
    }
  };

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
    if (files.length > 0 && extractionGraph && !contentIdError) {
      setLoading(true);
      try {
        const uploadPromises = files.map(file => 
          client.uploadFile(extractionGraph, file, labels, newContentId || undefined)
        );
        const results = await Promise.all(uploadPromises);
        console.log('Upload results:', results);
        handleClose();
        window.location.reload();
      } catch (error) {
        console.error('Error during upload:', error);
      } finally {
        setLoading(false);
      }
    }
  };

  const isUploadButtonDisabled = files.length === 0 || !extractionGraph || loading || !!contentIdError;

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
          <OutlinedInput
            label="Extraction Graph"
            value={extractionGraph}
            fullWidth
            notched={false}
            disabled
            sx={{ backgroundColor: "white", mt: 2 }}
            size="small"
          />
          <OutlinedInput
            label="Content Id (optional)"
            value={newContentId}
            onChange={(event) => validateContentId(event.target.value)}
            error={!!contentIdError}
            fullWidth
            notched={false}
            placeholder="Content Id (optional, hex string)"
            sx={{ backgroundColor: "white", mt: 2 }}
            size="small"
          />
          {contentIdError && (
            <FormHelperText error>{contentIdError}</FormHelperText>
          )}
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
       <Button onClick={handleOpen} size="small" variant="outlined" startIcon={<UploadIcon />} sx={{ color: "#3296FE"}}>
        Upload
      </Button>
    </>
  );
};

export default UploadButton;
