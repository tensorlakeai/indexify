import UploadIcon from '@mui/icons-material/Upload'
import { Box, Button, IconButton, Modal, Typography } from '@mui/material'
import { IExtractionGraph } from 'getindexify'
import { useState } from 'react'

interface props {
  extractionGraph: IExtractionGraph
}

const UploadButton = ({ extractionGraph }: props) => {
  const [open, setOpen] = useState(false)

  const handleOpen = () => setOpen(true)
  const handleClose = () => setOpen(false)

  const modalStyle = {
    position: 'absolute' as 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: 400,
    bgcolor: 'background.paper',
    border: '2px solid #000',
    boxShadow: 24,
    p: 4,
  }

  return (
    <>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="modal-modal-title"
        aria-describedby="modal-modal-description"
      >
        <Box sx={modalStyle}>
          <Typography id="modal-modal-title" variant="h6" component="h2">
            Upload Content
          </Typography>
          <Typography id="modal-modal-description" sx={{ mt: 2 }}>
            Duis mollis, est non commodo luctus, nisi erat porttitor ligula.
          </Typography>
        </Box>
      </Modal>
      <IconButton onClick={() => setOpen(true)}>
        <UploadIcon />
      </IconButton>
    </>
  )
}

export default UploadButton
