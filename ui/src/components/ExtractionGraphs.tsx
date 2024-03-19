import { IExtractionPolicy } from "getindexify";
import { Alert, Chip, Paper, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import React, { ReactElement } from "react";
import GavelIcon from "@mui/icons-material/Gavel";
import ExtractionPolicyItem from "./ExtractionPolicyItem";
import { IExtractionGraphCol, IExtractionGraphColumns } from "../types";


const ExtractionPoliciesTable = ({
  extractionPolicies,
}: {
  extractionPolicies: IExtractionPolicy[];
}) => {
  const cols: IExtractionGraphColumns = {
    name: { displayName: "Name", width: 300 },
    extractor: { displayName: "Extractor", width: 300 },
    inputParams: { displayName: "Input Params", width: 250 },
  };

  const renderHeader = () => {
    return (
      <Stack direction={"row"} pb={2}>
        {Object.values(cols).map((col: IExtractionGraphCol) => {
          return (
            <Box key={col.displayName} width={`${col.width}px`}>
              <Typography variant="h6">{col.displayName}</Typography>
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

    policies
      .filter((policy) => policy.content_source === source)
      .forEach((policy) => {
        items.push(
          <ExtractionPolicyItem
            extractionPolicy={policy}
            cols={cols}
            depth={depth}
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
        <Paper sx={{ p: 2 }}>
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

export default ExtractionPoliciesTable;
