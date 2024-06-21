import React, { useCallback, useState } from 'react';
import { 
  Box, 
  List, 
  ListItem, 
  ListItemText,
  Paper,
  Button,
  Typography,
  Divider
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import RemoveCircleIcon from '@mui/icons-material/RemoveCircle';
import { styled } from '@mui/system';
import InsertDriveFileOutlinedIcon from '@mui/icons-material/InsertDriveFileOutlined';

interface FileItem {
  name: string;
  size: number;
}

interface FileDropZoneProps {
  onFileSelect: (files: File[]) => void;
}

const FileInput = styled('input')({
  display: 'none',
});

function truncateFilename(filename: string) {
  if (filename.length > 25) {
    return filename.substring(0, 15) + '...';
  }
  return filename;
}

const FileDropZone: React.FC<FileDropZoneProps> = ({ onFileSelect }) => {
  const [files, setFiles] = useState<FileItem[]>([]);

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    const newFiles = Array.from(event.dataTransfer.files).map(file => ({
      name: file.name,
      size: file.size
    }));
    setFiles([...files, ...newFiles]);
    onFileSelect(Array.from(event.dataTransfer.files));
  };

  const handleRemove = useCallback((index: number) => {
    setFiles((prevFiles) => prevFiles.filter((_, i) => i !== index));
  }, []);

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files) {
      const newFiles = Array.from(e.target.files).map(file => ({
        name: file.name,
        size: file.size
      }));
      setFiles((prevFiles) => [...prevFiles, ...newFiles]);
      onFileSelect(Array.from(e.target.files));
    }
  }, [onFileSelect]);

  return (
    <Box
      sx={{
        border: '2px dashed #B0D1F7',
        borderRadius: '8px',
        p: 2,
        minHeight: 100,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'stretch',
        width: '100%'
      }}
      mt={2}
      onDrop={handleDrop}
      onDragOver={(event) => event.preventDefault()}
    >
      <Paper
        elevation={0}
        sx={{
          flexGrow: 1,
          overflow: 'auto',
          width: '100%',
          maxHeight: 300,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center'
        }}
        className="upload-modal-paper"
      >
        {files.length === 0 && (
            <>
            <FileInput
                id="file-input"
                type="file"
                multiple
                onChange={handleChange}
            />
            <label htmlFor="file-input">
                <Button
                    variant="outlined"
                    startIcon={<UploadFileIcon />}
                    sx={{ border: 1, borderColor: '#DFE5ED', color: '#3296FE' }}
                    component="span"
                >
                    Choose File
                </Button>
            </label>
            <Typography variant="caption" gutterBottom sx={{ marginTop: 1, color: "#757A82" }}>
                Drag and drop files here, or choose files
            </Typography>
            </>
        )}
        <List sx={{ width: "100%" }}>
          {files.map((file, index) => (
            <React.Fragment key={index}>
              <ListItem
                sx={{ borderBottom: '1px solid var(--Brand-BrandColor-05, #E5EFFB)', display: 'flex', justifyContent: 'space-between'}}
              >
                <div className="upload-modal-filename">
                  <div className="upload-modal-fileicon">
                      <InsertDriveFileOutlinedIcon sx={{color: '#3296FE'}} />
                  </div>
                  <ListItemText>
                     <Typography component="span" sx={{fontWeight: 500}}>{truncateFilename(file.name)} </Typography> | {(file.size / 1024).toFixed(2)} kb
                  </ListItemText>
                </div>
                <Button
                  variant="outlined"
                  startIcon={<RemoveCircleIcon />}
                  sx={{ border: 1, borderColor: '#DFE5ED', color: '#B91C1C', marginLeft: '8px' }}
                  onClick={() => handleRemove(index)}
                  size='small'
                  color='error'
                >
                  Remove
                </Button>
              </ListItem>
              <Divider component="li" />
            </React.Fragment>
          ))}
        </List>
      </Paper>
    </Box>
  );
};

export default FileDropZone;
