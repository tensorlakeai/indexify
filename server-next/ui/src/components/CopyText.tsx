import { Box, IconButton, Tooltip } from "@mui/material";
import ContentCopy from "@mui/icons-material/ContentCopy";
import { useState } from "react";

const CopyText = ({
  text,
  color,
  className,
  tooltipTitle = "Copy to clipboard",
  copiedTooltipTitle = "Copied!"
}: {
  text: string;
  color?: string;
  className?: string;
  tooltipTitle?: string;
  copiedTooltipTitle?: string;
}) => {
  const [showAlert, setShowAlert] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setShowAlert(true);
  };
  return (
    <Box className={className}>
      <Tooltip title={showAlert ? copiedTooltipTitle : tooltipTitle}>
        <IconButton onClick={handleCopy}>
          <ContentCopy sx={{ height: "20px" }} />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default CopyText;
