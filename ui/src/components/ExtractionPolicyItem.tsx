import { Box, Chip, Stack, Typography } from "@mui/material";
import { IExtractionPolicy, IIndex } from "getindexify";
import { IExtractionGraphColumns } from "../types";
import { Link } from "react-router-dom";

const ExtractionPolicyItem = ({
  extractionPolicy,
  namespace,
  cols,
  depth,
  index,
}: {
  extractionPolicy: IExtractionPolicy;
  namespace: string;
  cols: IExtractionGraphColumns;
  depth: number;
  index?: IIndex;
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

  const renderIndexSchema = () => {
    {
      if (!index?.schema) {
        return <Typography variant="body1">None</Typography>;
      }
      return (
        <Box sx={{ overflowX: "scroll" }}>
          <Stack gap={1} direction="row">
            {Object.keys(index.schema).map((val: string) => {
              return <Chip key={val} label={`${val}:${index.schema[val]}`} />;
            })}
          </Stack>
        </Box>
      );
    }
  };

  const LShapedLine = () => {
    const verticalLength = 36; 
    const horizontalLength = 20;

    return (
      <svg
        height={verticalLength + 10}
        width={horizontalLength + 5}
        style={{
          marginLeft: "-35px",
          marginTop: "-25px",
          position: "absolute",
        }}
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
          sx={{ minWidth: cols.name.width, pl: depth * 4 }}
          variant="body1"
        >
          {depth > 0 && <LShapedLine />}
          {extractionPolicy.name}
        </Typography>
        <Typography variant="body1" sx={{ minWidth: cols.extractor.width }}>
          {extractionPolicy.extractor}
        </Typography>
        <Box sx={{ minWidth: cols.inputParams.width }}>
          {renderInputParams()}
        </Box>
        {index && (
          <Box sx={{ minWidth: cols.indexName.width }}>
            <Link to={`/${namespace}/indexes/${index.name}`}>{index.name}</Link>
          </Box>
        )}
        {index && (
          <Box sx={{ minWidth: cols.schema.width, pr: 2 }}>
            {renderIndexSchema()}
          </Box>
        )}
      </Stack>
    </Box>
  );
};

export default ExtractionPolicyItem;
