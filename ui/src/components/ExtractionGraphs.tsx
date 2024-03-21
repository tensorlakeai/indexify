import { IExtractionPolicy, IIndex, ISchema } from "getindexify";
import { Alert, Chip, Typography } from "@mui/material";
import { Box, Stack } from "@mui/system";
import React, { ReactElement } from "react";
import GavelIcon from "@mui/icons-material/Gavel";
import ExtractionPolicyItem from "./ExtractionPolicyItem";
import { IExtractionGraphCol, IExtractionGraphColumns } from "../types";

const ExtractionGraphs = ({
  extractionPolicies,
  namespace,
  indexes,
  schemas,
}: {
  extractionPolicies: IExtractionPolicy[];
  namespace: string;
  indexes: IIndex[];
  schemas: ISchema[];
}) => {
  const itemheight = 60;
  const cols: IExtractionGraphColumns = {
    name: { displayName: "Name", width: 350 },
    extractor: { displayName: "Extractor", width: 250 },
    inputParams: { displayName: "Input Params", width: 300 },
    indexName: { displayName: "Index", width: 200 },
  };

  const renderSchema = (schema: ISchema, depth: number) => {
    return (
      <Box
        sx={{
          width: "auto",
          overflowX: "auto",
          pl: depth * 4,
          height: itemheight,
          display: "flex",
          alignItems: "center",
        }}
      >
        <Box
          sx={{
            backgroundColor: "#ebebeb",
            borderRadius: 3,
            // border:'1px solid black',
            pl: 1,
            pr: 0.5,
            py: 0.5,
          }}
        >
          <Typography sx={{ ml: 0, color: "#060D3F" }} variant="labelSmall">
            {schema.content_source} schema:
          </Typography>
          {Object.keys(schema.columns).map((val) => (
            <Chip
              key={val}
              sx={{ backgroundColor: "#060D3F", color: "white", ml: 1 }}
              label={`${val}: ${schema.columns[val]}`}
            />
          ))}
        </Box>
      </Box>
    );
  };

  const renderHeader = () => {
    return (
      <Stack
        direction={"row"}
        px={2}
        py={2}
        sx={{ backgroundColor: "white", borderBottom: "1px solid #e5e5e5" }}
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

  const getIndexFromPolicyName = (name: string): IIndex | undefined => {
    return indexes.find((v) => v.name === `${name}.embedding`);
  };

  const renderGraphItems = (
    policies: IExtractionPolicy[],
    source: string,
    depth = 0
  ): ReactElement[] => {
    let items: ReactElement[] = [];
    const schema = schemas.find(
      (v) => v.content_source === source && !!Object.keys(v.columns).length
    );
    if (schema) {
      items.push(renderSchema(schema, depth));
    }
    // use sibling count to keep track of how many are above
    let siblingCount = items.length;
    policies
      .filter((policy) => policy.content_source === source)
      .forEach((policy, i) => {
        items.push(
          <ExtractionPolicyItem
            key={policy.name}
            extractionPolicy={policy}
            namespace={namespace}
            cols={cols}
            depth={depth}
            siblingCount={siblingCount}
            index={getIndexFromPolicyName(policy.name)}
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
          maxWidth: "100%",
          overflow: "auto",
          border: "1px solid rgba(224, 224, 224, 1);",
          borderRadius: "5px",
        }}
      >
        {renderHeader()}
        <Box sx={{ p: 2, backgroundColor: "white" }}>
          {renderGraphItems(extractionPolicies, "ingestion")}
        </Box>
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
