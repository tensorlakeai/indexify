import { Box, IconButton, Tooltip } from "@mui/material";
import ContentCopy from "@mui/icons-material/ContentCopy";
import { useState } from "react";

interface CopyTextProps {
  text: string;
  color?: string;
  className?: string;
  tooltipTitle?: string;
  copiedTooltipTitle?: string;
}

export function CopyText({
  text,
  className,
  tooltipTitle = "Copy to clipboard",
  copiedTooltipTitle = "Copied!"
}: CopyTextProps) {
  const [isCopied, setIsCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), 2000);
    } catch (error) {
      console.error('Failed to copy text:', error);
    }
  };

  return (
    <Box className={className}>
      <Tooltip title={isCopied ? copiedTooltipTitle : tooltipTitle}>
        <IconButton onClick={handleCopy} size="small">
          <ContentCopy sx={{ height: 20 }} />
        </IconButton>
      </Tooltip>
    </Box>
  );
}

export default CopyText;
