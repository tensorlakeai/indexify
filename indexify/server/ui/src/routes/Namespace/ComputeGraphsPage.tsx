import { Box, Alert } from "@mui/material";
import { useLoaderData } from "react-router-dom";
import { ComputeGraphsCard } from "../../components/cards/ComputeGraphsCard";
import type { ComputeGraphLoaderData } from "./types";

const ComputeGraphsPage = () => {
  const { client, computeGraphs, namespace } = useLoaderData() as ComputeGraphLoaderData;

  if (!client || !computeGraphs || !namespace) {
    return (
      <Box>
        <Alert severity="error">
          Failed to load compute graphs data. Please try again.
        </Alert>
      </Box>
    );
  }

  return (
    <Box>
      <ComputeGraphsCard
        computeGraphs={computeGraphs}
        client={client}
        namespace={namespace}
      />
    </Box>
  );
};

export default ComputeGraphsPage;
