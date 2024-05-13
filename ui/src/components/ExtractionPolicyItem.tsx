import { Box, Chip, Stack, Typography } from "@mui/material";
import { IExtractionPolicy, IExtractor, IIndex, ITask } from "getindexify";
import { IExtractionGraphColumns } from "../types";
import { Link } from "react-router-dom";

const ExtractionPolicyItem = ({
  extractionPolicy,
  siblingCount,
  namespace,
  cols,
  depth,
  itemHeight,
  extractors,
  tasks,
  index,
}: {
  extractionPolicy: IExtractionPolicy;
  siblingCount: number;
  namespace: string;
  cols: IExtractionGraphColumns;
  depth: number;
  itemHeight: number;
  extractors: IExtractor[];
  tasks: ITask[];
  index?: IIndex;
}) => {
  const renderInputParams = () => {
    if (
      !extractionPolicy.input_params ||
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

  const renderMimeTypes = () => {
    const extractor = extractors.find(
      (extractor) => extractor.name === extractionPolicy.extractor
    );
    if (!extractor) return null;

    return (
      <Box sx={{ overflowX: "scroll", maxWidth:`calc(${cols.mimeTypes.width}px - 10px)` }}>
        <Stack gap={1} direction="row">
          {(extractor.input_mime_types ?? []).map((val: string) => {
            return (
              <Chip
                key={val}
                label={val}
                sx={{ backgroundColor: "#4AA4F4", color: "white" }}
              />
            );
          })}
        </Stack>
      </Box>
    );
  };

  const LShapedLine = () => {
    const verticalLength = 30 + siblingCount * itemHeight;
    const horizontalLength = 20;

    return (
      <svg
        height={verticalLength + 10}
        width={horizontalLength + 5}
        style={{
          marginLeft: "-35px",
          marginTop: `${12 - verticalLength}px`,
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
    <Box sx={{ py: 0.5, position: "relative", height: 40 }}>
      <Stack direction={"row"} sx={{ display: "flex", alignItems: "center" }}>
        <Typography
          sx={{ minWidth: cols.name.width, pl: depth * 4 }}
          variant="body1"
        >
          {depth > 0 && <LShapedLine />}
          <Link
            to={`/${namespace}/extraction-policies/${extractionPolicy.graph_name}/${extractionPolicy.name}`}
          >
            {extractionPolicy.name}
          </Link>
        </Typography>
        <Typography variant="body1" sx={{ minWidth: cols.extractor.width }}>
          {extractionPolicy.extractor}
        </Typography>
        <Box sx={{ minWidth: cols.mimeTypes.width }}>{renderMimeTypes()}</Box>
        <Box sx={{ minWidth: cols.inputParams.width }}>
          {renderInputParams()}
        </Box>
        <Box sx={{ minWidth: cols.taskCount.width }}>
          {tasks.filter(task => task.extraction_policy_id === extractionPolicy.id).length}
        </Box>
      </Stack>
    </Box>
  );
};

export default ExtractionPolicyItem;
