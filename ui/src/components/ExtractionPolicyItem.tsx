import { Box, Chip, Stack, Typography } from "@mui/material";
import { IExtractionPolicy } from "getindexify";
import { IExtractionGraphColumns } from "../types";

const ExtractionPolicyItem = ({
  extractionPolicy,
  cols,
  depth,
}: {
  extractionPolicy: IExtractionPolicy;
  cols: IExtractionGraphColumns;
  depth: number;
}) => {
  const renderInputParams = () => {
    if (
      extractionPolicy.input_params === undefined ||
      Object.keys(extractionPolicy.input_params).length === 0
    ) {
      return <Chip label={`none`} />;
    }
    const params = extractionPolicy.input_params;
    return (
      <Box sx={{ overflowX: "scroll" }}>
        <Stack gap={1} direction="row">
          {Object.keys(params).map((val: string) => {
            return <Chip key={val} label={`${val}:${params[val]}`} />;
          })}
        </Stack>
      </Box>
    );
  };

  const LShapedLine = ({ depth }: { depth: number }) => {
    // Calculate the length of the line based on the depth, for example
    const verticalLength = 36; // Adjust based on your needs
    const horizontalLength = 20; // The horizontal length increases with depth

    return (
      <svg
        height={verticalLength + 10}
        width={horizontalLength + 5}
        style={{ marginLeft:'-35px', marginTop: "-25px", position:"absolute" }}
      >
        {/* Vertical line */}
        <line
          x1="5"
          y1="0"
          x2="5"
          y2={verticalLength}
          style={{ stroke: "#8D8D8D", strokeWidth: 2 }}
        />
        {/* Horizontal line */}
        <line
          x1="5"
          y1={verticalLength}
          x2={horizontalLength + 5}
          y2={verticalLength}
          style={{ stroke: "#8D8D8D", strokeWidth: 2 }}
        />
      </svg>
    );
  };

  return (
    <Box sx={{ py: 1, position: "relative" }}>
      <Stack direction={"row"} sx={{ display: "flex", alignItems: "center" }}>
        <Typography
          sx={{ width: cols.name?.width ?? "250px", pl: depth * 4 }}
          variant="label"
        >
          {depth > 0 && <LShapedLine depth={depth}></LShapedLine>}
          {extractionPolicy.name}
        </Typography>
        <Typography
          variant="body1"
          sx={{ width: cols.extractor?.width ?? "250px" }}
        >
          {extractionPolicy.extractor}
        </Typography>
        <Box sx={{ width: cols.inputParams?.width ?? "250px" }}>
          {renderInputParams()}
        </Box>
      </Stack>
    </Box>
  );
};

export default ExtractionPolicyItem;
