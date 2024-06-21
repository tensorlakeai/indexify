import { Box, IconButton, Tooltip } from "@mui/material";
import ContentCopy from "@mui/icons-material/ContentCopy";
import { useState } from "react";

const CopyText = ({
  text,
  color,
}: {
  text: string;
  color?: string;
}) => {
  const [showAlert, setShowAlert] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setShowAlert(true);
  };
  return (
    <Box>
      <Tooltip title={showAlert ? "Copied!" : "Copy to clipboard"}>
        <IconButton onClick={handleCopy}>
          <ContentCopy sx={{ color, height:'20px' }} />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default CopyText;
