import React, { useState, useCallback } from 'react';
import { Box, Typography, Button } from '@mui/material';
import { styled } from '@mui/system';
import UploadFileIcon from '@mui/icons-material/UploadFile';

const DropZone = styled(Box)(({ theme }) => ({
  border: `2px dashed #B0D1F7`,
  borderRadius: theme.shape.borderRadius,
  paddingTop: 28,
  paddingBottom: 28,
  textAlign: 'center',
  cursor: 'pointer',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
}));

const FileInput = styled('input')({
  display: 'none',
});

interface FileDropZoneProps {
  onFileSelect: (file: File) => void;
}

const FileDropZone: React.FC<FileDropZoneProps> = ({ onFileSelect }) => {
  const [dragActive, setDragActive] = useState(false);

  const handleDrag = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      onFileSelect(e.dataTransfer.files[0]);
    }
  }, [onFileSelect]);

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      onFileSelect(e.target.files[0]);
    }
  }, [onFileSelect]);

  return (
    <DropZone
      onDragEnter={handleDrag}
      onDragLeave={handleDrag}
      onDragOver={handleDrag}
      onDrop={handleDrop}
      sx={{ backgroundColor: dragActive ? 'action.hover' : 'background.paper', display: 'flex', flexDirection: 'column', alignItems: 'center', marginTop: 2 }}
    >
      <label htmlFor="file-input">
        <Button
          variant="outlined"
          startIcon={<UploadFileIcon />}
          sx={{ border: 1, borderColor: '#B0D1F7', color: '#3296FE' }}
        >
          Choose File
        </Button>
      </label>
      <Typography variant="caption" gutterBottom sx={{ marginTop: 2 }}>
        Drag and drop file here, or choose file
      </Typography>
      <FileInput
        id="file-input"
        type="file"
        onChange={handleChange}
      />
    </DropZone>
  );
};

export default FileDropZone;