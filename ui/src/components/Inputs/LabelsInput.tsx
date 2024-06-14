import { Box, Button, TextField, IconButton, Typography } from '@mui/material'
import { Add, Delete } from '@mui/icons-material'
import { useState } from 'react'

interface LabelsInputProps {
  onChange: (labels: Record<string, string>) => void
}

const LabelsInput = ({ onChange }: LabelsInputProps) => {
  const [labels, setLabels] = useState<Record<string, string>>({})
  const [newKey, setNewKey] = useState('')
  const [newValue, setNewValue] = useState('')

  const handleAddLabel = () => {
    if (newKey && newValue) {
      const updatedLabels = { ...labels, [newKey]: newValue }
      setLabels(updatedLabels)
      onChange(updatedLabels)
      setNewKey('')
      setNewValue('')
    }
  }

  const handleDeleteLabel = (key: string) => {
    const { [key]: _, ...remainingLabels } = labels
    setLabels(remainingLabels)
    onChange(remainingLabels)
  }

  return (
    <Box>
      <Typography variant="h6" component="h2" sx={{ mt: 2 }}>
        Labels
      </Typography>
      <Box display="flex" gap={1} sx={{ mt: 2 }}>
        <TextField
          label="Key"
          value={newKey}
          onChange={(e) => setNewKey(e.target.value)}
          variant="outlined"
        />
        <TextField
          label="Value"
          value={newValue}
          onChange={(e) => setNewValue(e.target.value)}
          variant="outlined"
        />
        <IconButton color="primary" onClick={handleAddLabel}>
          <Add />
        </IconButton>
      </Box>
      {Object.entries(labels).map(([key, value]) => (
        <Box display="flex" alignItems="center" gap={1} key={key} sx={{ mt: 2 }}>
          <TextField
            label="Key"
            value={key}
            InputProps={{
              readOnly: true,
            }}
            variant="outlined"
          />
          <TextField
            label="Value"
            value={value}
            InputProps={{
              readOnly: true,
            }}
            variant="outlined"
          />
          <IconButton color="secondary" onClick={() => handleDeleteLabel(key)}>
            <Delete />
          </IconButton>
        </Box>
      ))}
    </Box>
  )
}

export default LabelsInput
