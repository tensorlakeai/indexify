import {
  IExtractionGraph,
  IExtractionPolicy,
  IExtractor,
  ITask,
} from "getindexify";
import { Alert, IconButton, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import React, { ReactElement } from "react";
import GavelIcon from "@mui/icons-material/Gavel";
import InfoIcon from "@mui/icons-material/Info";
import ExtractionPolicyItem from "./ExtractionPolicyItem";
import { IExtractionGraphCol, IExtractionGraphColumns } from "../types";

const ExtractionGraphs = ({
  extractionGraphs,
  namespace,
  extractors,
  tasks,
}: {
  extractionGraphs: IExtractionGraph[];
  namespace: string;
  extractors: IExtractor[];
  tasks: ITask[];
}) => {
  const itemheight = 60;
  const cols: IExtractionGraphColumns = {
    name: { displayName: "Name", width: 350 },
    extractor: { displayName: "Extractor", width: 225 },
    mimeTypes: { displayName: "Input MimeTypes", width: 225},
    inputParams: { displayName: "Input Parameters", width: 225 },
    taskCount: { displayName: "Tasks", width: 75 },
  };
  
  const renderHeader = () => {
    return (
      <Stack
        direction={"row"}
        px={2}
        py={2}
        sx={{
          width: "100%",
          borderBottom: "1px solid #e5e5e5",
        }}
      >
        {Object.values(cols).map((col: IExtractionGraphCol) => {
          return (
            <Box key={col.displayName} minWidth={`${col.width}px`}>
              <Typography variant="label">{col.displayName}</Typography>
            </Box>
          );
        })}
      </Stack>
    );
  };

  const renderGraphItems = (
    policies: IExtractionPolicy[],
    source: string,
    depth = 0
  ): ReactElement[] => {
    let items: ReactElement[] = [];
    // use sibling count to keep track of how many are above
    let siblingCount = items.length;
    policies
      .filter((policy) => policy.content_source === source)
      .forEach((policy, i) => {
        items.push(
          <ExtractionPolicyItem
            key={policy.name}
            tasks={tasks}
            extractionPolicy={policy}
            namespace={namespace}
            cols={cols}
            extractors={extractors}
            depth={depth}
            siblingCount={siblingCount}
            itemHeight={itemheight}
          />
        );
        const children = renderGraphItems(policies, policy.name, depth + 1);
        items = items.concat(children);
        siblingCount = children.length;
      });
    return items;
  };

  const renderContent = () => {
    if (extractionGraphs.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Graphs Found
          </Alert>
        </Box>
      );
    }

    return (
      <Box
        sx={{
          width: "100%",
          overflow: "auto",
          border: "1px solid rgba(224, 224, 224, 1);",
          borderRadius: "5px",
          backgroundColor: "white",
        }}
      >
        <div style={{ minWidth: "max-content" }}>{renderHeader()}</div>
        {extractionGraphs.map((graph) => {
          return (
            <Box key={graph.name} sx={{ p: 2 }}>
              <Typography variant="h3">{graph.name}</Typography>
              {renderGraphItems(graph.extraction_policies, "")}
            </Box>
          );
        })}
      </Box>
    );
  };

  return (
    <>
      <Stack
        display={"flex"}
        direction={"row"}
        alignItems={"center"}
        spacing={2}
      >
        <GavelIcon />
        <Typography variant="h3">
          Extraction Graphs
          <IconButton
            href="https://getindexify.ai/concepts/#extraction-graphs"
            target="_blank"
          >
            <InfoIcon fontSize="small" />
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractionGraphs;
