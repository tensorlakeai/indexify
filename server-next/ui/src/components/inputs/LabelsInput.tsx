import { Box, IconButton, Typography, OutlinedInput } from "@mui/material";
import AddCircleIcon from "@mui/icons-material/AddCircle";
import RemoveCircleIcon from "@mui/icons-material/RemoveCircle";
import { useState } from "react";

interface LabelsInputProps {
  onChange: (labels: Record<string, string>) => void;
  disabled: boolean;
}

const LabelsInput = ({ onChange, disabled }: LabelsInputProps) => {
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [newKey, setNewKey] = useState("");
  const [newValue, setNewValue] = useState("");
  const entries = Object.entries(labels || {});
  const lastKey = entries.length > 0 ? entries[entries.length - 1][0] : null;

  const handleAddLabel = () => {
    if (newKey && newValue) {
      const updatedLabels = { ...labels, [newKey]: newValue };
      setLabels(updatedLabels);
      onChange(updatedLabels);
      setNewKey("");
      setNewValue("");
    }
  };

  const handleDeleteLabel = (key: string) => {
    const { [key]: _, ...remainingLabels } = labels;
    setLabels(remainingLabels);
    onChange(remainingLabels);
  };

  const handleChange = (
    setValue: React.Dispatch<React.SetStateAction<string>>
  ) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const regex = /^[a-zA-Z0-9-_]*$/;
    if (regex.test(e.target.value)) {
      setValue(e.target.value);
    }
  };

  return (
    <Box>
      <Typography variant="h6" component="h2" sx={{ mt: 2 }}>
        Labels
      </Typography>
      <Box sx={{ backgroundColor: "#F7F9FC", borderRadius: "8px" }}>
        <Box
          display="flex"
          gap={1}
          sx={{
            mt: 2,
            padding: "12px",
            ...Object.entries(labels).length > 0 && { paddingBottom: "0px" }
          }}
        >
          <OutlinedInput
            disabled={disabled}
            label="Key"
            value={newKey}
            onChange={handleChange(setNewKey)}
            fullWidth
            notched={false}
            placeholder="Key"
            sx={{ backgroundColor: "white" }}
            size="small"
          />
          <OutlinedInput
            disabled={disabled}
            label="Value"
            value={newValue}
            onChange={handleChange(setNewValue)}
            sx={{ backgroundColor: "white" }}
            placeholder="Value"
            fullWidth
            notched={false}
            size="small"
          />
          <IconButton
            disabled={disabled}
            color="primary"
            onClick={handleAddLabel}
          >
            <AddCircleIcon color="info" />
          </IconButton>
        </Box>
        {Object.entries(labels).map(([key, value]) =>
          <Box
            display="flex"
            alignItems="center"
            gap={1}
            key={key}
            sx={{
              mt: 2,
              paddingLeft: "12px",
              paddingRight: "12px",
              ...key === lastKey && { paddingBottom: "12px" }
            }}
          >
            <OutlinedInput
              label="Key"
              value={key}
              inputProps={{
                readOnly: true
              }}
              sx={{ backgroundColor: "white" }}
              placeholder="Key"
              fullWidth
              notched={false}
              size="small"
            />
            <OutlinedInput
              label="Value"
              value={value}
              inputProps={{
                readOnly: true
              }}
              sx={{ backgroundColor: "white" }}
              placeholder="Value"
              fullWidth
              notched={false}
              size="small"
            />
            <IconButton
              disabled={disabled}
              color="secondary"
              onClick={() => handleDeleteLabel(key)}
            >
              <RemoveCircleIcon color="error" />
            </IconButton>
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default LabelsInput;
