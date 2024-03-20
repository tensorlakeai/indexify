import { IExtractionPolicy, IIndex } from "getindexify";
import { Alert, Paper, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import React, { ReactElement } from "react";
import GavelIcon from "@mui/icons-material/Gavel";
import ExtractionPolicyItem from "./ExtractionPolicyItem";
import { IExtractionGraphCol, IExtractionGraphColumns } from "../types";

const ExtractionGraphs = ({
  extractionPolicies,
  namespace,
  indexes,
}: {
  extractionPolicies: IExtractionPolicy[];
  namespace: string;
  indexes: IIndex[];
}) => {
  const cols: IExtractionGraphColumns = {
    name: { displayName: "Name", width: 300 },
    extractor: { displayName: "Extractor", width: 250 },
    inputParams: { displayName: "Input Params", width: 200 },
    indexName: { displayName: "Index", width: 250 },
    schema: { displayName: "Schema", width: 220 },
  };

  const renderHeader = () => {
    return (
      <Stack direction={"row"} pb={2}>
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
  
  const getIndexFromPolicyName = (name: string): IIndex | undefined => {
    return indexes.find((v) => v.name === `${name}.embedding`);
  };

  const renderGraphItems = (
    policies: IExtractionPolicy[],
    source: string,
    depth = 0
  ): ReactElement[] => {
    let items: ReactElement[] = [];

    policies
      .filter((policy) => policy.content_source === source)
      .forEach((policy) => {
        items.push(
          <ExtractionPolicyItem
            key={policy.name}
            extractionPolicy={policy}
            namespace={namespace}
            cols={cols}
            depth={depth}
            index={getIndexFromPolicyName(policy.name)}
          />
        );
        const children = renderGraphItems(policies, policy.name, depth + 1);
        items = items.concat(children);
      });
    return items;
  };

  const renderContent = () => {
    if (extractionPolicies.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Policies Found
          </Alert>
        </Box>
      );
    }

    return (
      <Box
        sx={{
          width: "100%",
        }}
      >
        <Paper
          sx={{
            maxWidth: "100%",
            overflow: "auto",
            p: 2,
          }}
        >
          {renderHeader()}
          {renderGraphItems(extractionPolicies, "ingestion")}
        </Paper>
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
        <Typography variant="h3">Extraction Graphs</Typography>
      </Stack>
      {renderContent()}
    </>
  );
};

export default ExtractionGraphs;
