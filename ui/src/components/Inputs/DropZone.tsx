import React, { useCallback, useState } from 'react';
import { 
  Box, 
  List, 
  ListItem, 
  ListItemText,
  Paper ,
  Button,
  Typography
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import RemoveCircleIcon from '@mui/icons-material/RemoveCircle';
import Divider from '@mui/material/Divider';
import { styled } from '@mui/system';
import InsertDriveFileOutlinedIcon from '@mui/icons-material/InsertDriveFileOutlined';

interface FileItem {
  name: string;
  size: number;
}

interface FileDropZoneProps {
  onFileSelect: (file: File) => void;
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
  };

  const handleRemove = (index: number) => {
    const updatedFiles = files.filter((_, i) => i !== index);
    setFiles(updatedFiles);
  };

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      onFileSelect(e.target.files[0]);
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
        </>
        )}
        <List sx={{ width: "100%" }}>
          {files.map((file, index) => (
            <>
            <ListItem
              key={index}
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
            </>
          ))}
        </List>
      </Paper>
    </Box>
  );
};

export default FileDropZone;