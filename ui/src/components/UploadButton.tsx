import UploadIcon from '@mui/icons-material/Upload'
import {
  Box,
  Button,
  Modal,
  Typography,
  MenuItem,
  Select,
  Paper,
  CircularProgress,
} from '@mui/material'
import { ExtractionGraph, IndexifyClient } from 'getindexify'
import { useState } from 'react'
import LabelsInput from './Inputs/LabelsInput'

interface Props {
  client: IndexifyClient
}

const UploadButton = ({ client }: Props) => {
  const [open, setOpen] = useState(false)
  const [file, setFile] = useState<Blob | null>(null)
  const [labels, setLabels] = useState<Record<string, string>>({})
  const [fileName, setFileName] = useState('')
  const [extractionGraphName, setExtractionGraphName] = useState('')
  const [loading, setLoading] = useState(false)
  const [extractionGraphs, setExtractionGraphs] = useState<ExtractionGraph[]>(
    client.extractionGraphs
  )

  const handleOpen = () => setOpen(true)
  const handleClose = () => setOpen(false)

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files[0]) {
      setFile(event.target.files[0])
      setFileName(event.target.files[0].name)
    }
  }

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
  }

  const upload = async () => {
    if (file && extractionGraphName) {
      setLoading(true)
      await client.uploadFile(extractionGraphName, file, labels)
      window.location.reload()
    }
  }

  const updateExtractionGraphs = async () => {
    const graphs = await client.getExtractionGraphs()
    setExtractionGraphs(graphs)
  }

  return (
    <>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="modal-modal-title"
        aria-describedby="modal-modal-description"
      >
        <Paper sx={modalStyle}>
          <Typography id="modal-modal-title" variant="h6" component="h2">
            Upload Content
          </Typography>
          <Typography id="modal-modal-description" sx={{ mt: 2 }}>
            Select a file to upload and choose an extraction graph.
          </Typography>
          <Select
            disabled={loading}
            onFocus={updateExtractionGraphs}
            value={extractionGraphName}
            onChange={(e) => setExtractionGraphName(e.target.value)}
            displayEmpty
            fullWidth
            sx={{ mt: 2 }}
          >
            <MenuItem value="" disabled>
              Select Extraction Graph
            </MenuItem>
            {extractionGraphs.map((graph) => (
              <MenuItem key={graph.name} value={graph.name}>
                {graph.name}
              </MenuItem>
            ))}
          </Select>
          <Box display="flex" alignItems={'center'} gap={2}>
            <Button
              disabled={loading}
              variant="contained"
              component="label"
              sx={{ mt: 2 }}
            >
              Choose File
              <input type="file" hidden onChange={handleFileChange} />
            </Button>
            {fileName && (
              <Typography variant="body2" sx={{ mt: 1 }}>
                Selected File: {fileName}
              </Typography>
            )}
          </Box>
          <LabelsInput
            disabled={loading}
            onChange={(val) => {
              setLabels(val)
            }}
          />

          <Box sx={{ mt: 2, position: 'relative' }}>
            <Button
              variant="contained"
              onClick={upload}
              disabled={!file || !extractionGraphName || loading}
            >
              Upload
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
      <Button onClick={handleOpen} size="small" endIcon={<UploadIcon />}>
        Upload
      </Button>
    </>
  )
}

export default UploadButton
